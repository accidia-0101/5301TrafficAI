# hls_sync_test.py —— HLS 推流 + 检测同步小测试（从头开始）
import asyncio
import os
from events.bus import AsyncBus, Detection
from events.frame_discrete import run_frame_source_raw, run_sampler_equal_time
from events.Accident_detect.incident_aggregator import AccidentAggregator
from events.Accident_detect.accident_detector import run_accident_detector
from events.stream_hls import HLSPusher, HLSTarget  # 需要你已按前面提供的实现好 stream_hls.py
from events.unit_test.web_helptest import run_server
# ========= 可调参数 =========
VIDEO_PATH = r"E:\Training\Recording 2025-10-30 172929.mp4"
CAMERA_ID  = "cam-1"

# HLS 输出到本地目录（前端用静态服务器映射这个目录，例如映射为 /hls/cam-1/）
HLS_OUT_DIR = r"E:\Training\hls_out\cam-1"
HLS_SEG_TIME = 1.0         # 1s 分片，端到端延迟更低
HLS_LIST_SIZE = 6          # 清单项个数

SAMPLE_FPS = 15.0          # 供检测用的采样帧率（不影响推流）
DECISION_THRESH = 0.65
DEVICE = 0                 # YOLO 设备

# 事件聚合参数（与你的 detector_test 保持一致）
AGG_ALPHA = 0.25
AGG_ENTER_THR = 0.65
AGG_EXIT_THR = 0.40
AGG_MIN_PERSIST_FRAMES = 3
AGG_MIN_END_FRAMES = 8
AGG_OCCLUSION_GRACE = 3.0
AGG_MERGE_GAP = 5.0
AGG_REQUIRED_HAP = 3
AGG_USE_EMA_OPEN = False
IDLE_TIMEOUT_SEC = 2.0  # 连续这么久没收到 detections，就认为源结束并强制收尾

async def run_event_aggregator(bus: AsyncBus, camera_id: str):
    async with bus.subscribe("detections", mode="fifo", maxsize=256) as q_det:
        agg = AccidentAggregator(
            camera_id=camera_id,
            alpha=AGG_ALPHA,
            enter_thr=AGG_ENTER_THR,
            exit_thr=AGG_EXIT_THR,
            min_persistence_frames=AGG_MIN_PERSIST_FRAMES,
            min_end_frames=AGG_MIN_END_FRAMES,
            occlusion_grace_sec=AGG_OCCLUSION_GRACE,
            merge_gap_sec=AGG_MERGE_GAP,
            required_happened_consecutive=AGG_REQUIRED_HAP,
            use_ema_open=AGG_USE_EMA_OPEN,
        )
        print(f"🧮 Aggregator ON (no-control, idle-timeout={IDLE_TIMEOUT_SEC}s)")

        try:
            while True:
                try:
                    det = await asyncio.wait_for(q_det.get(), timeout=IDLE_TIMEOUT_SEC)
                except asyncio.TimeoutError:
                    # 认为源结束：没有新检测了 → flush 补关并退出
                    print("⏳ detections idle → assume EOF → flush & session_end")
                    for ev in agg.flush():
                        print("🧮 [flush->events]:", ev)
                        await bus.publish("events", ev)
                    await bus.publish("events", {"type": "session_end", "camera_id": camera_id, "reason": "idle-timeout"})
                    return

                if getattr(det, "camera_id", None) != camera_id:
                    continue

                open_ev, close_evs = agg.update(
                    ts=getattr(det, "pts_in_video", det.ts_unix),
                    conf=det.confidence,
                    frame_ok=True,
                    happened=det.happened,
                    frame_idx=getattr(det, "frame_idx", None),
                )
                if open_ev is not None:
                    print("🧮 [open]->events:", open_ev)
                    await bus.publish("events", open_ev)
                for ev in close_evs:
                    print("🧮 [close]->events:", ev)
                    await bus.publish("events", ev)
                await asyncio.sleep(0)
        finally:
            print("🧮 Aggregator finally → flush()")
            for ev in agg.flush():
                print("🧮 [final-flush]->events:", ev)
                await bus.publish("events", ev)


async def run_print_detections(bus: AsyncBus):
    async with bus.subscribe("detections") as q:
        counter = 0
        while True:
            det: Detection = await q.get()
            counter += 1
            if counter % 5 == 0:
                print(f"[检测日志] 已收到 {counter} 次检测结果")
            if det.type == "accident" and det.happened:
                print(f"[!!!] 事故候选 | cam={det.camera_id} | conf={det.confidence:.3f} | "
                      f"pts={getattr(det, 'pts_in_video', 0.0):.3f}s | "
                      f"fidx={getattr(det, 'frame_idx', -1)}")
            else:
                print(f"🔹 正常帧 | conf={det.confidence:.3f} | "
                      f"pts={getattr(det, 'pts_in_video', 0.0):.3f}s")
            await asyncio.sleep(0)

async def run_print_events(bus: AsyncBus):
    async with bus.subscribe("events") as q:
        while True:
            ev = await q.get()
            if ev["type"] == "accident_open":
                ts = ev.get("ts") or ev.get("ts_unix", 0.0)
                print(f"🚨 事故开始 | cam={ev['camera_id']} | id={ev['incident_id']} "
                      f"| ts={ts:.3f}s | start_fidx={ev.get('start_frame_idx')}")
            elif ev["type"] == "accident_close":
                print(f"✅ 事故结束 | cam={ev['camera_id']} | id={ev['incident_id']} "
                      f"| 持续={ev.get('duration_sec',0):.2f}s | 峰值={ev.get('peak_confidence',0):.3f} "
                      f"| 阳性帧={ev.get('pos_frames',0)} "
                      f"| [{ev.get('start_frame_idx')} → {ev.get('end_frame_idx')}]")
            await asyncio.sleep(0)


async def main():
    print("🚀 启动 HLS+检测 同步小测试（从头播放 + 从头检测 + 统一 PTS）")
    print(f"🎥 视频源: {VIDEO_PATH}\n📷 摄像头ID: {CAMERA_ID}\n🧠 模型阈值: {DECISION_THRESH} | 设备: {DEVICE}")
    print(f"📡 HLS 输出: {os.path.join(HLS_OUT_DIR, 'index.m3u8')}  （请用静态服务映射给前端）")

    bus = AsyncBus()
    web_task = asyncio.create_task(run_server(host="127.0.0.1", port=8000, hls_root=HLS_OUT_DIR.rsplit("\\", 1)[0]))
    # 1) HLS 推流器：订阅 frames_raw，从 0 秒开始写清单（目录自动清空）
    hls = HLSPusher(
        bus, CAMERA_ID,
        target=HLSTarget(
            out_dir=HLS_OUT_DIR,
            m3u8_name="index.m3u8",
            seg_time=HLS_SEG_TIME,
            list_size=HLS_LIST_SIZE,
            cleanup_on_start=True,   # 确保每次会话从头播放

        ),
        enc_fps=None,                # 自动根据 pts 估计源 FPS，更贴近文件时间轴
        force_size=None,             # 如需统一分辨率可设 (1280, 720) / (1280, 960)
        prefer_nvenc=True,           # 5070Ti 建议启用
        gop_seconds=2.0,
    )
    # ===== 启动顺序：先聚合，再解码/采样/检测，避免订阅竞态 =====
    t_web = web_task
    t_hls = asyncio.create_task(hls.run())
    t_agg = asyncio.create_task(run_event_aggregator(bus, CAMERA_ID))  # 先起！
    t_src = asyncio.create_task(run_frame_source_raw(bus, CAMERA_ID, VIDEO_PATH))  # 再起源
    t_spl = asyncio.create_task(run_sampler_equal_time(bus, CAMERA_ID, target_fps=SAMPLE_FPS))
    t_det = asyncio.create_task(run_accident_detector(bus, decision_thresh=DECISION_THRESH, device=DEVICE))
    t_pev = asyncio.create_task(run_print_events(bus))
    t_pdet = asyncio.create_task(run_print_detections(bus))

    tasks = [
        t_web,
        t_hls,                                        # 推流（frames_raw）
        t_agg, # 聚合
        t_src,  # 采样 → frames
        t_spl,  # frames → detections
        t_det,             # detections → events
        t_pev,
        t_pdet
    ]

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass
    finally:
        await hls.stop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 手动中止。")
