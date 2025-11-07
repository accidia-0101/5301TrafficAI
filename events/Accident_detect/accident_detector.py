# detector_accident.py
"""
YOLOv8 单类事故检测（固定使用训练好的 best.pt）

输入：
- 订阅 'frames'（注意：这是“等时采样后”的帧流）
- 每帧需带 frame_idx 与 pts_in_video（由采样器/帧源填充）

输出：
- 发布到 'detections'，同时携带 frame_idx / pts_in_video，方便前端或叠加对齐 HLS 播放时间
"""

from __future__ import annotations
import asyncio, time
from typing import Optional
import numpy as np
from events.bus import Frame, Detection, AsyncBus

MODEL_PATH = r"E:\PythonProject\DjangoTrafficAI\events\pts\best.pt"

class AccidentDetector:
    """YOLOv8 单类事故检测引擎"""
    def __init__(
        self,
        *,
        imageSize: int = 960,
        yolo_conf: float = 0.05,
        yolo_iou: float = 0.50,
        device: Optional[str | int] = 0,  # 默认 GPU:0；改 "cpu" 可走 CPU
    ):
        try:
            from ultralytics import YOLO
        except Exception as e:
            raise RuntimeError("缺少 ultralytics，请先 pip install ultralytics") from e

        self.imageSize = imageSize
        self.yolo_conf = yolo_conf
        self.yolo_iou = yolo_iou
        self.device = device

        print(f"🔹 正在加载模型权重: {MODEL_PATH}")
        self._yolo = YOLO(MODEL_PATH)
        if hasattr(self._yolo, "overrides"):
            self._yolo.overrides["conf"] = yolo_conf
            self._yolo.overrides["iou"] = yolo_iou
            self._yolo.overrides["device"] = device

        # 可选：空张量预热，降低首帧抖动（GPU 时更明显）
        try:
            dummy = np.zeros((self.imageSize, self.imageSize, 3), dtype=np.uint8)
            _ = self._yolo.predict(dummy, imgsz=self.imageSize, conf=self.yolo_conf, iou=self.yolo_iou, verbose=False, device=self.device)
        except Exception:
            pass

    def infer_frame_conf(self, rgb: np.ndarray) -> float:
        """
        单帧推理 → 帧级置信度：
          - 无框：0.0
          - 有框：max(boxes.conf)
        """
        res = self._yolo.predict(
            rgb,
            imgsz=self.imageSize,
            conf=self.yolo_conf,
            iou=self.yolo_iou,
            verbose=False,
            device=self.device,
        )[0]

        boxes = getattr(res, "boxes", None)
        if boxes is None or len(boxes) == 0:
            return 0.0
        confs = boxes.conf
        if confs is None or len(confs) == 0:
            return 0.0
        return float(confs.max().item())


async def run_accident_detector(
    bus: AsyncBus,
    *,
    decision_thresh: float = 0.65,  # 帧级判定阈值
    imgsz: int = 960,
    yolo_conf: float = 0.05,
    yolo_iou: float = 0.50,
    device: Optional[str | int] = 0,
):
    """
    订阅 'frames'（采样后的帧流）→ YOLO 推理 → 发布 'detections'
    发布的 Detection 携带 frame_idx 与 pts_in_video，保证与 HLS 时间轴对齐。
    """
    engine = AccidentDetector(
        imageSize=imgsz,
        yolo_conf=yolo_conf,
        yolo_iou=yolo_iou,
        device=device,
    )

    loop = asyncio.get_running_loop()

    # ✅ 正确的异步上下文写法
    async with bus.subscribe("frames") as q:
        while True:
            frame: Frame = await q.get()

            # ✅ 将推理放在线程池里，避免阻塞 asyncio 循环
            frame_conf = await loop.run_in_executor(None, engine.infer_frame_conf, frame.rgb)
            happened = frame_conf >= decision_thresh

            det = Detection(
                type="accident",
                camera_id=frame.camera_id,
                ts_unix=frame.ts_unix,
                happened=happened,
                confidence=frame_conf,
                frame_idx=getattr(frame, "frame_idx", 0),
                pts_in_video=getattr(frame, "pts_in_video", 0.0),
            )
            await bus.publish("detections", det)
            await asyncio.sleep(0)

