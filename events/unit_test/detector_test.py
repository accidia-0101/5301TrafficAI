# detector_test.py
import asyncio
import time

from events.Accident_detect.accident_detector import run_accident_detector
from events.bus import AsyncBus, Detection

# ==== 修改这里 ====
VIDEO_PATH = r"E:\Training\Recording 2025-10-30 172929.mp4"  # ← 你的本地视频
CAMERA_ID = "cam-1"
TARGET_FPS = 60
DECISION_THRESH = 0.65
DEVICE = 0   # "cpu" 没GPU就改成cpu
# ==================

async def run_print_detections(bus: AsyncBus):
    q = bus.subscribe("detections")

    counter = 0
    while True:
        det: Detection = await q.get()
        counter += 1
        if counter % 5 == 0:
            print(f"[检测日志] 已收到 {counter} 次检测结果")
        if det.type == "accident" and det.happened:
            print(f"✅ 检测到事故 | 摄像头={det.camera_id} | 置信度={det.confidence:.3f} | 时间戳={det.ts_unix:.3f}")
        else:
            print(f"🔹 正常帧 | conf={det.confidence:.3f}")
        await asyncio.sleep(0)

# ---- 重写 run_frame_source 增强日志（仅调试用）----
async def run_frame_source_debug(bus: AsyncBus, camera_id: str, url_or_path: str, target_fps: float = 45.0):
    import cv2, os
    print(f"🎥 打开视频源: {url_or_path}")
    cap = cv2.VideoCapture(url_or_path)
    if not cap.isOpened():
        print("❌ 无法打开视频源！")
        return

    interval = 1.0 / max(1e-3, target_fps)
    last_emit = 0.0
    is_file = os.path.exists(url_or_path)
    frame_count = 0
    start = time.time()

    try:
        while True:
            ok, bgr = cap.read()
            if not ok:
                if is_file:
                    print("🔚 视频读取完毕。")
                    break
                await asyncio.sleep(0.02)
                continue

            now = time.time()
            if now - last_emit < interval:
                continue
            last_emit = now
            frame_count += 1
            if frame_count % 10 == 0:
                print(f"[取帧日志] 已读取 {frame_count} 帧")

            import numpy as np, cv2
            rgb = cv2.cvtColor(bgr, cv2.COLOR_BGR2RGB)
            from events.bus import Frame
            frame = Frame(camera_id=camera_id, ts_unix=now, rgb=rgb)
            await bus.publish("frames", frame)
            await asyncio.sleep(0)

    finally:
        cap.release()
        dur = time.time() - start
        print(f"✅ 视频结束，共读取 {frame_count} 帧，用时 {dur:.1f} 秒")


async def main():
    print("🚀 启动 TrafficAI 检测调试")
    bus = AsyncBus()
    tasks = [
        asyncio.create_task(run_frame_source_debug(bus, CAMERA_ID, VIDEO_PATH, target_fps=TARGET_FPS)),
        asyncio.create_task(run_accident_detector(
            bus,
            decision_thresh=DECISION_THRESH,
            device=DEVICE,
        )),
        asyncio.create_task(run_print_detections(bus)),
    ]

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n手动中止。")
