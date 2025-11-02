# verify_frames_bus.py
import os, cv2, time, asyncio
from dataclasses import dataclass
import numpy as np

# --------- 定义数据结构 & 事件总线 ----------
@dataclass
class Frame:
    camera_id: str
    ts_unix: float
    rgb: np.ndarray  # HxWx3, uint8, RGB

@dataclass
class Detection:
    type: str          # accident/weather
    camera_id: str
    ts_unix: float
    happened: bool
    confidence: float  # 可为 0.0 表示未知

class AsyncBus:
    def __init__(self):
        self.topics: dict[str, list[asyncio.Queue]] = {}

    def subscribe(self, topic: str) -> asyncio.Queue:
        q = asyncio.Queue(maxsize=64)
        self.topics.setdefault(topic, []).append(q)
        return q

    async def publish(self, topic: str, item):
        qs = self.topics.get(topic, [])
        if not qs:
            return
        async def _safe_put(queue: asyncio.Queue):
            try:
                await asyncio.wait_for(queue.put(item), timeout=0.1)  # 100ms 容忍
            except asyncio.TimeoutError:
                # 丢弃本次投递，避免背压
                pass
        await asyncio.gather(*(_safe_put(q) for q in qs))

# --------- 取流/切帧 → 发布到 "frames" ----------
async def run_frame_source(bus: AsyncBus, camera_id: str, url_or_path: str, target_fps: float = 15.0):
    # 支持 "0" 打开默认摄像头；否则按文件/URL 处理
    cap = cv2.VideoCapture(int(url_or_path)) if url_or_path.isdigit() else cv2.VideoCapture(url_or_path)
    try:
        interval = 1.0 / max(1e-3, target_fps)
        last = 0.0
        is_file = (not url_or_path.isdigit()) and os.path.exists(url_or_path)
        while True:
            ok, bgr = cap.read()
            if not ok:
                if is_file:
                    break  # 文件播完
                await asyncio.sleep(0.02)
                continue
            now = time.time()
            if now - last < interval:
                # 通过丢帧逼近 target_fps，防止积压
                continue
            last = now
            rgb = cv2.cvtColor(bgr, cv2.COLOR_BGR2RGB)
            await bus.publish("frames", Frame(camera_id=camera_id, ts_unix=now, rgb=rgb))
            await asyncio.sleep(0)  # 让出事件循环
    finally:
        cap.release()
        cv2.destroyAllWindows()

# --------- 订阅 "frames" 并打印 ----------
async def frames_probe(bus: AsyncBus, max_frames: int = 50):
    q = bus.subscribe("frames")
    start = time.time()
    count = 0
    while True:
        f: Frame = await q.get()
        count += 1
        print(f"[{count:03d}] cam={f.camera_id} ts={f.ts_unix:.3f} shape={f.rgb.shape} dtype={f.rgb.dtype}")
        if count >= max_frames:
            elapsed = time.time() - start
            fps = count / elapsed if elapsed > 0 else 0.0
            print(f"\n✅ 收到 {count} 帧，用时 {elapsed:.2f}s，约 {fps:.1f} fps（验证：切帧已进入广播队列）")
            return

# --------- 主函数 ----------
async def main():
    VIDEO_PATH = os.getenv(r"E:\Training\Recording 2025-10-30 172929.mp4", "0")  # 改成你的文件路径，如 "D:/videos/test.mp4"；"0" 表示默认摄像头
    CAMERA_ID = os.getenv("CAMERA_ID", "cam01")

    # 基本存在性检查（文件不存在又不是摄像头数字时提示）
    if (not VIDEO_PATH.isdigit()) and (not os.path.exists(VIDEO_PATH)):
        print(f"❌ 找不到视频文件：{VIDEO_PATH}")
        return

    bus = AsyncBus()
    producer = asyncio.create_task(run_frame_source(bus, CAMERA_ID, VIDEO_PATH, target_fps=15.0))
    consumer = asyncio.create_task(frames_probe(bus, max_frames=30))

    # 等待打印验证完成后结束
    await consumer
    # 验证结束后取消取流任务（如果是摄像头/RTSP会一直读）
    producer.cancel()
    try:
        await producer
    except asyncio.CancelledError:
        pass

if __name__ == "__main__":
    asyncio.run(main())
