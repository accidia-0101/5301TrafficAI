
# frame_discrete.py
"""
视频帧源 & 采样器（解耦版）
- run_frame_source_raw：不降帧，发布 frames_raw
- run_sampler_equal_time：等时采样，订阅 frames_raw → 发布 frames
- 兼容：保留原来的 run_frame_source（未来修改可以根据简单已有代码依赖）
"""
import cv2, time, asyncio, os
from events.bus import Frame, AsyncBus

async def run_frame_source_raw(bus: AsyncBus, camera_id: str, url_or_path: str):
    """
    不降帧：解码每一帧，发布到 'frames_raw'
    - frame_idx 从0开始计数
    - pts_in_video：文件源用 frame_idx/src_fps；直播源用单调时钟近似
    """
    cap = cv2.VideoCapture(url_or_path)
    try:
        is_file = os.path.exists(url_or_path)
        src_fps = cap.get(cv2.CAP_PROP_FPS) or 0.0
        src_fps = src_fps if src_fps and src_fps < 1000 else 0.0  # 有些容器会返回诡异的大数
        start_mono = time.monotonic()
        start_wall = time.time()
        frame_idx = 0

        while True:
            ok, bgr = cap.read()
            if not ok:
                if is_file:
                    break
                await asyncio.sleep(0.01)  # 直播断帧，等一会儿
                continue

            # 统一用 RGB
            rgb = cv2.cvtColor(bgr, cv2.COLOR_BGR2RGB)

            # 计算 PTS：文件 → 用 fps；直播 → 用单调时钟相对增量
            if src_fps > 0:
                pts = frame_idx / src_fps
            else:
                pts = time.monotonic() - start_mono

            f = Frame(
                camera_id=camera_id,
                ts_unix=time.time(),
                rgb=rgb,
                frame_idx=frame_idx,
                pts_in_video=pts
            )
            await bus.publish("frames_raw", f)
            frame_idx += 1

            # 不主动 sleep；让出事件循环以便其他协程运行
            await asyncio.sleep(0)
    finally:
        cap.release()
        try:
            cv2.destroyAllWindows()
        except Exception:
                pass


async def run_sampler_equal_time(bus: AsyncBus, camera_id: str, target_fps: float = 15.0, jitter_epsilon: float = 1e-4):
    """
    等时采样（时间网格）：
      订阅 frames_raw（同一 camera），按固定时间步长 1/target_fps 选帧
      → 发布到 'frames'（分析订阅）
    - 适配 CFR/VFR：统一基于 pts_in_video 判断
    """
    step = 1.0 / max(1e-3, target_fps)
    next_t = None

    async with bus.subscribe("frames_raw") as sub:
        while True:
            f: Frame = await sub.get()
            if f.camera_id != camera_id:
                continue

            if next_t is None:
                # 对齐第一帧的 pts
                # 也可选择对齐到 floor(pts/step)*step 以与切片秒边界更一致
                next_t = f.pts_in_video

            # 用 while 保证“跳秒”时能连发多帧，保持时间网格不漂移
            emitted = False
            while f.pts_in_video + jitter_epsilon >= next_t:
                await bus.publish("frames", f)
                next_t += step
                emitted = True

            if not emitted:
                await asyncio.sleep(0)


# 兼容保留：原来的“为分析降帧”的函数（若已有地方直接用它）
async def run_frame_source(bus: AsyncBus, camera_id: str, url_or_path: str, target_fps: float = 60.0):
    """
    原函数：为分析而降帧（发布 'frames'）
    ——保留以兼容旧逻辑；新架构建议用 run_frame_source_raw + run_sampler_equal_time
    """
    cap = cv2.VideoCapture(url_or_path)
    try:
        interval = 1.0 / max(1e-3, target_fps)
        last_emit = 0.0
        is_file = os.path.exists(url_or_path)
        src_fps = cap.get(cv2.CAP_PROP_FPS) or 0.0
        src_fps = src_fps if src_fps and src_fps < 1000 else 0.0
        frame_idx = 0
        start_mono = time.monotonic()

        while True:
            ok, bgr = cap.read()
            if not ok:
                if is_file:
                    break
                await asyncio.sleep(0.02)
                continue

            now = time.time()
            if now - last_emit < interval:
                await asyncio.sleep(0)
                frame_idx += 1
                continue

            last_emit = now
            rgb = cv2.cvtColor(bgr, cv2.COLOR_BGR2RGB)
            pts = (frame_idx / src_fps) if src_fps > 0 else (time.monotonic() - start_mono)
            frame = Frame(camera_id=camera_id, ts_unix=now, rgb=rgb, frame_idx=frame_idx, pts_in_video=pts)
            await bus.publish("frames", frame)
            frame_idx += 1
            await asyncio.sleep(0)
    finally:
        cap.release()
        try:
            cv2.destroyAllWindows()
        except Exception:
            pass
