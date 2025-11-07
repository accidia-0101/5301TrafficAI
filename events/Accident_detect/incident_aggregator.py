# events/logic/accident_aggregator.py
from dataclasses import dataclass
from typing import Optional, List, Tuple

@dataclass
class IncidentState:
    incident_id: str
    camera_id: str
    start_ts: float          # 以“视频内 PTS 秒”为准（HLS 对齐）
    end_ts: float
    peak_conf: float
    pos_frames: int
    # 新增：帧级对齐字段（可选但推荐）
    start_fidx: Optional[int] = None
    end_fidx: Optional[int] = None

class AccidentAggregator:
    """
    逐帧 -> 事件(open/close)
    - 开案(严格模式)：必须连续 N 帧 happened=True 才开（默认 N=3）
    - 关案：EMA <= exit_thr 且连续 min_end_frames 阴性
    - 其它：遮挡宽限、合并窗口

    【同步要点】
    - ts 参数现在默认传 “pts_in_video”（相对视频起点秒），与 HLS 播放器 currentTime 天然对齐
    - 同时传 frame_idx，便于帧级对齐与回放
    """
    def __init__(
        self,
        camera_id: str,
        alpha: float = 0.25,
        enter_thr: float = 0.65,
        exit_thr: float = 0.40,
        min_persistence_frames: int = 3,
        min_end_frames: int = 8,
        occlusion_grace_sec: float = 1.0,
        merge_gap_sec: float = 5.0,
        required_happened_consecutive: int = 3,
        use_ema_open: bool = False,
    ):
        self.camera_id = camera_id
        self.alpha = alpha
        self.enter_thr = enter_thr
        self.exit_thr = exit_thr
        self.min_persistence_frames = min_persistence_frames
        self.min_end_frames = min_end_frames
        self.occlusion_grace_sec = occlusion_grace_sec
        self.merge_gap_sec = merge_gap_sec
        self.required_happened_consecutive = max(1, required_happened_consecutive)
        self.use_ema_open = use_ema_open

        # 状态
        self.ema = 0.0
        self._pos_streak = 0
        self._neg_streak = 0
        self._hap_streak = 0
        self._first_pos_ts: Optional[float] = None
        self._first_hap_ts: Optional[float] = None
        self._first_pos_fidx: Optional[int] = None    # 新增：记录连击首帧帧号
        self._first_hap_fidx: Optional[int] = None
        self._open: Optional[IncidentState] = None
        self._last_seen_ts: Optional[float] = None
        self._last_end_ts: Optional[float] = None
        self._counter = 0

    def _new_id(self) -> str:
        self._counter += 1
        return f"{self.camera_id}-{self._counter:06d}"

    def update(
        self,
        ts: float,                 # 现在默认传 pts_in_video（与 HLS 对齐）
        conf: float,
        *,
        frame_ok: bool = True,
        happened: bool = False,
        frame_idx: Optional[int] = None,   # 新增：帧号（便于帧级对齐）
    ) -> Tuple[Optional[dict], List[dict]]:
        open_event = None
        close_events: List[dict] = []

        # 1) EMA
        self.ema = self.alpha * conf + (1 - self.alpha) * self.ema

        # 2) 可见性/遮挡
        if frame_ok:
            self._last_seen_ts = ts

        # 3) 计数
        if self._open is None:
            if happened:
                if self._hap_streak == 0:
                    self._first_hap_ts = ts
                    self._first_hap_fidx = frame_idx
                self._hap_streak += 1
            else:
                self._hap_streak = 0
                self._first_hap_ts = None
                self._first_hap_fidx = None

        thr = self.exit_thr if self._open is not None else self.enter_thr
        is_pos = (self.ema >= thr)
        if is_pos:
            if self._pos_streak == 0:
                self._first_pos_ts = ts
                self._first_pos_fidx = frame_idx
            self._pos_streak += 1
            self._neg_streak = 0
        else:
            self._neg_streak += 1
            if self._open is None:
                self._first_pos_ts = None
                self._first_pos_fidx = None
                self._pos_streak = 0

        # 4) 开案
        if self._open is None:
            strict_open = (self._hap_streak >= self.required_happened_consecutive)
            ema_open = self.use_ema_open and (self.ema >= self.enter_thr) and (self._pos_streak >= self.min_persistence_frames)

            if strict_open or ema_open:
                reuse_last_start = (self._last_end_ts is not None and (ts - self._last_end_ts) <= self.merge_gap_sec)
                incident_id = self._new_id()

                # 回溯开案起点：严格通道用 first_hap，备用通道用 first_pos
                base_start_ts = (self._first_hap_ts if strict_open else self._first_pos_ts) or ts
                base_start_fidx = (self._first_hap_fidx if strict_open else self._first_pos_fidx)

                start_ts = self._last_end_ts if reuse_last_start else base_start_ts
                start_fidx = base_start_fidx if not reuse_last_start else None  # 合并窗口回溯到上次结束，帧号未知可置 None

                self._open = IncidentState(
                    incident_id=incident_id,
                    camera_id=self.camera_id,
                    start_ts=start_ts,
                    end_ts=ts,
                    peak_conf=conf,
                    pos_frames=1,
                    start_fidx=start_fidx,
                    end_fidx=frame_idx,
                )
                open_event = {
                    "type": "accident_open",
                    "incident_id": incident_id,
                    "camera_id": self.camera_id,
                    "ts": start_ts,                # ★ 视频内 PTS 秒
                    "start_frame_idx": start_fidx,  # ★ 帧号
                    "confidence": float(conf),
                }
                # 清理
                self._first_hap_ts = None
                self._first_hap_fidx = None
                self._first_pos_ts = None
                self._first_pos_fidx = None
                self._hap_streak = 0

        else:
            # 5) 已开案：更新尾部与峰值
            self._open.end_ts = ts
            self._open.end_fidx = frame_idx
            if conf > self._open.peak_conf:
                self._open.peak_conf = conf
            if is_pos or happened:
                self._open.pos_frames += 1

            # 6) 结束判定
            grace_ok = True
            if self._last_seen_ts is not None and (ts - self._last_seen_ts) > self.occlusion_grace_sec:
                grace_ok = False
            if (self._neg_streak >= self.min_end_frames) and grace_ok:
                close_events.append({
                    "type": "accident_close",
                    "incident_id": self._open.incident_id,
                    "camera_id": self.camera_id,
                    "start_ts": self._open.start_ts,     # 与 HLS 对齐的时间轴
                    "end_ts": self._open.end_ts,
                    "start_frame_idx": self._open.start_fidx,
                    "end_frame_idx": self._open.end_fidx,
                    "duration_sec": max(0.0, self._open.end_ts - self._open.start_ts),
                    "peak_confidence": float(self._open.peak_conf),
                    "pos_frames": int(self._open.pos_frames),
                })
                self._last_end_ts = self._open.end_ts
                self._open = None
                self._pos_streak = 0
                self._neg_streak = 0
                self._first_pos_ts = None
                self._first_pos_fidx = None
                self._first_hap_ts = None
                self._first_hap_fidx = None
                self._hap_streak = 0

        return open_event, close_events

    def flush(self) -> List[dict]:
        if self._open is None:
            return []
        inc = self._open
        self._open = None
        self._last_end_ts = inc.end_ts
        self._pos_streak = 0
        self._neg_streak = 0
        self._first_pos_ts = None
        self._first_pos_fidx = None
        self._first_hap_ts = None
        self._first_hap_fidx = None
        self._hap_streak = 0
        return [{
            "type": "accident_close",
            "incident_id": inc.incident_id,
            "camera_id": inc.camera_id,
            "start_ts": inc.start_ts,
            "end_ts": inc.end_ts,
            "start_frame_idx": inc.start_fidx,
            "end_frame_idx": inc.end_fidx,
            "duration_sec": max(0.0, inc.end_ts - inc.start_ts),
            "peak_confidence": float(inc.peak_conf),
            "pos_frames": int(inc.pos_frames),
        }]
