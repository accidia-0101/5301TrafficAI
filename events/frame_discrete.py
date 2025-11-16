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
from events.bus import Frame, AsyncBus, topic_for
import cv2, time, asyncio, os

async def run_frame_source_raw(bus: AsyncBus, camera_id: str, url_or_path: str):
    """
    Non-downsampled video source: decode frames sequentially and publish to frames_raw:<camera_id>
    - Each frame includes frame_idx and pts_in_video (video timestamp in seconds)
    """

    cap = cv2.VideoCapture(url_or_path, cv2.CAP_FFMPEG)
    try:
        is_file = os.path.exists(url_or_path)
        src_fps = cap.get(cv2.CAP_PROP_FPS) or 0.0
        src_fps = src_fps if src_fps and src_fps < 1000 else 0.0
        start_mono = time.monotonic()
        frame_idx = 0

        while True:
            ok, bgr = cap.read()
            if not ok:
                # Exit when the file ends; for live sources, wait briefly
                if is_file:
                    break
                await asyncio.sleep(0.01)
                continue

            rgb = cv2.cvtColor(bgr, cv2.COLOR_BGR2RGB)
            pts = frame_idx / src_fps if src_fps > 0 else (time.monotonic() - start_mono)

            f = Frame(
                camera_id=camera_id,
                ts_unix=time.time(),
                rgb=rgb,
                frame_idx=frame_idx,
                pts_in_video=pts,
            )

            # Partitioned publish: frames_raw:<camera_id>
            await bus.publish(topic_for("frames_raw", camera_id), f)
            frame_idx += 1

            # Prevent blocking the event loop
            await asyncio.sleep(0)

    finally:
        print(f"[frame_source] {camera_id} finished, releasing video")
        cap.release()
        try:
            cv2.destroyAllWindows()
        except Exception:
            pass


async def run_sampler_equal_time(bus: AsyncBus, camera_id: str, target_fps: float = 60.0, jitter_epsilon: float = 1e-4):
    """
    Equal-interval sampling: pull frames from frames_raw:<camera_id>, sample them uniformly
    according to the target FPS, and publish to frames:<camera_id>.
    """

    step = 1.0 / max(1e-3, target_fps)
    next_t = None

    topic_in = topic_for("frames_raw", camera_id)
    topic_out = topic_for("frames", camera_id)

    async with bus.subscribe(topic_in, mode="fifo", maxsize=64) as sub:
        while True:
            f: Frame = await sub.get()

            # Initialize sampling clock
            if next_t is None:
                next_t = f.pts_in_video

            emitted = False
            while f.pts_in_video + jitter_epsilon >= next_t:
                newf = Frame(
                    camera_id=f.camera_id,
                    ts_unix=f.ts_unix,
                    rgb=f.rgb,
                    frame_idx=f.frame_idx,
                    pts_in_video=f.pts_in_video,
                )
                await bus.publish(topic_for("frames", camera_id), newf)
                next_t += step

            if not emitted:
                await asyncio.sleep(0)
