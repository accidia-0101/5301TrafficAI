# -----------------------------------------------------------------------------
# Copyright (c) 2025
#
# Authors:
#   Liruo Wang
#       School of Electrical Engineering and Computer Science,
#       University of Ottawa
#       lwang032@uottawa.ca
#
# All rights reserved.
# -----------------------------------------------------------------------------
from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass
from typing import Dict, List

import numpy as np
import torch
from PIL import Image
from torchvision import transforms
from torchvision.models import mobilenet_v3_small

from events.bus import AsyncBus, Frame, topic_for

_MODEL_PATH = r"E:\PythonProject\DjangoTrafficAI\events\pts\weather_cls_2class.pth"
_DEVICE = "cuda"
_BATCH_SIZE = 4
_POLL_MS = 20
_INTERVAL_SEC = 300

ID2LABEL = {
    0: "clear",
    1: "rain",
}


@dataclass(slots=True)
class _Item:
    cam: str
    frame: Frame


class _WeatherEngine:
    """Wrapper for loading the CNN model weights."""

    def __init__(self):
        print(f"[weather-multi] loading weights: {_MODEL_PATH}")

        self.model = mobilenet_v3_small(weights=None, num_classes=2)
        state = torch.load(_MODEL_PATH, map_location=_DEVICE)
        self.model.load_state_dict(state)

        self.model.to(_DEVICE)
        self.model.eval()

        self.tf = transforms.Compose([
            transforms.ToTensor(),
            transforms.Resize((224, 224)),
        ])

    @torch.no_grad()
    def infer_batch(self, images: List[np.ndarray]):


        xs = []
        for img in images:
            pil = Image.fromarray(img)
            xs.append(self.tf(pil).unsqueeze(0))

        batch = torch.cat(xs, dim=0).to(_DEVICE)
        logits = self.model(batch)
        probs = torch.softmax(logits, dim=1)
        labels = probs.argmax(dim=1)

        out = []
        for l, p in zip(labels, probs):
            label = ID2LABEL[int(l)]
            conf = float(p[int(l)])
            out.append((label, conf))
        return out


async def run_weather_detector_multi(
    bus: AsyncBus,
    *,
    camera_ids: List[str],
    batch_size: int = _BATCH_SIZE,
    poll_ms: int = _POLL_MS,
    interval_sec: int = _INTERVAL_SEC,
):
    """
    Multi-stream weather detection:
    - Mirrors the structure the multi-stream accident detector
    - Subscribes to frames:<cam>
    - Uses round-robin to assemble batches
    - Runs batch inference with the CNN
    - For each camera:
        * Perform immediate detection on the first frame
        * Then detect once every interval_sec
    - Publish results to weather:<cam>
    """

    engine = _WeatherEngine()
    loop = asyncio.get_running_loop()


    bufs: Dict[str, deque[Frame]] = {cam: deque(maxlen=128) for cam in camera_ids}


    last_weather: Dict[str, str | None] = {cam: None for cam in camera_ids}


    next_time: Dict[str, float] = {cam: 0 for cam in camera_ids}

    async def _collector(cam: str):
        topic_in = topic_for("frames", cam)
        async with bus.subscribe(topic_in, mode="fifo", maxsize=64) as q:
            while True:
                f: Frame = await q.get()
                bufs[cam].append(f)

    collectors = [asyncio.create_task(_collector(cam)) for cam in camera_ids]

    try:
        while True:
            batch_items: List[_Item] = []
            cams_round = list(camera_ids)

            while len(batch_items) < batch_size and cams_round:
                cam = cams_round.pop(0)
                q = bufs[cam]
                if q:
                    frm = q.popleft()
                    batch_items.append(_Item(cam=cam, frame=frm))
                cams_round.append(cam)

                if all(len(bufs[c]) == 0 for c in camera_ids):
                    break

            if not batch_items:
                await asyncio.sleep(poll_ms / 1000)
                continue


            images = [it.frame.rgb for it in batch_items]


            results = await loop.run_in_executor(None, engine.infer_batch, images)


            for it, (label, conf) in zip(batch_items, results):
                cam = it.cam
                frame = it.frame
                ts = frame.ts_unix


                if ts < next_time[cam]:
                    continue
                next_time[cam] = ts + interval_sec


                if label == last_weather[cam]:
                    continue
                last_weather[cam] = label


                evt = {
                    "type": "weather",
                    "camera_id": cam,
                    "weather": label,
                    "confidence": conf,
                    "ts_unix": frame.ts_unix,
                    "frame_idx": frame.frame_idx,
                    "pts_in_video": frame.pts_in_video,
                }

                await bus.publish(topic_for("weather", cam), evt)
                print(f"[weather-multi] cam={cam} -> {label} ({conf:.3f})")

    finally:
        for t in collectors:
            t.cancel()
            try:
                await t
            except Exception:
                pass
