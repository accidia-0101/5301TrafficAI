# events/session_single.py
from __future__ import annotations
import asyncio
from typing import Optional, Callable, List

from events.bus import AsyncBus, Detection, Frame
from events.frame_discrete import run_frame_source_raw, run_sampler_equal_time
from events.stream_hls import HLSPusher, HLSTarget

# 可选：你的检测与聚合
from events.Accident_detect.accident_detector import run_accident_detector
from events.Accident_detect.incident_aggregator import AccidentAggregator


class SingleFileSession:
    """
    单路从头播放会话：
    - frames_raw → HLS（不降帧，不卡顿）
    - frames_raw → 等时采样 → frames（供检测）
    - frames → YOLO → detections → AccidentAggregator → 通过回调上报告警
    - 文件播完自动发 session_end 回调
    """

    def __init__(
        self,
        *,
        camera_id: str,
        file_path: str,
        hls_out_dir: str,
        sampler_fps: float = 15.0,             # 检测帧率
        enc_force_size: tuple[int, int] | None = None,
        prefer_nvenc: bool = True,
        decision_thresh: float = 0.65,
        device: int = 0,
        on_alert: Optional[Callable[[str, dict], None]] = None,   # 回调(event_name, payload)
        on_session_end: Optional[Callable[[dict], None]] = None,  # 回调(payload)
        bus: Optional[AsyncBus] = None,
    ):
        self.bus = bus or AsyncBus()
        self.camera_id = camera_id
        self.file_path = file_path
        self.hls_out_dir = hls_out_dir
        self.sampler_fps = sampler_fps
        self.enc_force_size = enc_force_size
        self.prefer_nvenc = prefer_nvenc

        self.decision_thresh = decision_thresh
        self.device = device
        self.on_alert = on_alert
        self.on_session_end = on_session_end

        self.tasks: List[asyncio.Task] = []
        self._hls: HLSPusher | None = None

        # 供 /status 查询
        self.last_pts_in_video: float = 0.0
        self.last_frame_idx: int = 0
        self._running: bool = False

    @property
    def running(self) -> bool:
        return self._running

    async def _track_status(self):
        """从 detections 或 frames 订阅，实时记录 last_pts/frame_idx（尽量轻量不阻塞）"""
        # 订阅 frames（稳定 CFR，必然有 pts/frame_idx）
        async with self.bus.subscribe("frames", mode="latest", maxsize=1) as q:
            while True:
                f: Frame = await q.get()
                if f.camera_id != self.camera_id:
                    continue
                self.last_pts_in_video = getattr(f, "pts_in_video", self.last_pts_in_video)
                self.last_frame_idx = getattr(f, "frame_idx", self.last_frame_idx)
                await asyncio.sleep(0)

    async def _aggregate_and_alert(self):
        """订阅 detections，聚合成 open/close 告警，通过回调上报"""
        async with self.bus.subscribe("detections") as q:
            agg = AccidentAggregator(
                camera_id=self.camera_id,
                alpha=0.25, enter_thr=0.65, exit_thr=0.40,
                min_persistence_frames=3, min_end_frames=8,
                occlusion_grace_sec=3.0, merge_gap_sec=5.0,
                required_happened_consecutive=3, use_ema_open=False
            )
            try:
                while True:
                    det: Detection = await q.get()
                    ts = getattr(det, "pts_in_video", det.ts_unix)
                    open_ev, close_evs = agg.update(
                        ts=ts, conf=det.confidence, frame_ok=True,
                        happened=det.happened, frame_idx=getattr(det, "frame_idx", None),
                    )
                    if open_ev is not None and self.on_alert:
                        self.on_alert("accident_open", {
                            "camera_id": self.camera_id,
                            "incident_id": open_ev["incident_id"],
                            "ts": open_ev.get("ts"),
                            "start_frame_idx": open_ev.get("start_frame_idx"),
                        })
                    for ev in close_evs:
                        if self.on_alert:
                            self.on_alert("accident_close", {
                                "camera_id": self.camera_id,
                                "incident_id": ev["incident_id"],
                                "duration_sec": ev.get("duration_sec"),
                                "peak_confidence": ev.get("peak_confidence"),
                                "pos_frames": ev.get("pos_frames"),
                                "start_frame_idx": ev.get("start_frame_idx"),
                                "end_frame_idx": ev.get("end_frame_idx"),
                            })
                    await asyncio.sleep(0)
            finally:
                for ev in agg.flush():
                    if self.on_alert:
                        self.on_alert("accident_close", ev)

    async def start(self):
        if self._running:
            return
        self._running = True

        # 1) HLS 推流器（清空目录，确保从头开始）
        self._hls = HLSPusher(
            self.bus, self.camera_id,
            target=HLSTarget(out_dir=self.hls_out_dir, seg_time=1.0, list_size=6, cleanup_on_start=True),
            enc_fps=None,                    # 自动估计源 FPS
            force_size=self.enc_force_size,
            prefer_nvenc=self.prefer_nvenc,
            gop_seconds=2.0,
        )
        t_hls = asyncio.create_task(self._hls.run())

        # 2) 原始帧源（文件从头解码 → frames_raw）
        t_src = asyncio.create_task(run_frame_source_raw(self.bus, self.camera_id, self.file_path))

        # 3) 等时采样（→ frames，用于检测订阅）
        t_spl = asyncio.create_task(run_sampler_equal_time(self.bus, self.camera_id, target_fps=self.sampler_fps))

        # 4) 检测（订阅 frames → 发布 detections）
        t_det = asyncio.create_task(
            run_accident_detector(self.bus, decision_thresh=self.decision_thresh, device=self.device)
        )

        # 5) 事件聚合 + 告警回调
        t_agg = asyncio.create_task(self._aggregate_and_alert())

        # 6) 状态跟踪（供 /status）
        t_stat = asyncio.create_task(self._track_status())

        # 7) 监控 EOF：raw 解码任务结束 = 文件播完
        async def _watch_eof():
            await asyncio.gather(t_src)   # t_src 完成即 EOF
            if self.on_session_end:
                self.on_session_end({"camera_id": self.camera_id, "reason": "eof"})
        t_eof = asyncio.create_task(_watch_eof())

        self.tasks = [t_hls, t_src, t_spl, t_det, t_agg, t_stat, t_eof]

    async def stop(self):
        if not self._running:
            return
        self._running = False

        # 先停 HLS
        if self._hls:
            await self._hls.stop()

        # 取消其他任务
        for t in self.tasks:
            if not t.done():
                t.cancel()
        for t in self.tasks:
            try:
                await t
            except asyncio.CancelledError:
                pass
        self.tasks.clear()
