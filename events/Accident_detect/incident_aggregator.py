"""
AccidentAggregator: aggregates per-frame detection results into stable accident events
(partitioned-topic version, rewritten)
--------------------------------------------------------------------
Subscribe: accident:<camera_id>              # Per-frame detection results (Detection)
Publish:   accidents.open:<camera_id>        # Accident-open event (once)
           accidents.close:<camera_id>       # Accident-close event (may be delayed due to merge window)

Design Highlights:
- Pure AsyncBus: partitioned subscription/publishing per camera to avoid cross-stream interference.
- Stable decision logic: EMA smoothing + strict 3 consecutive frames for opening + exit threshold +
  consecutive negative frames for closing.
- Occlusion grace: short frame gaps do not immediately trigger closing (_OCCLUSION_GRACE_SEC).
- Merge window: if a new accident opens within _MERGE_GAP_SEC after closure, it is merged as the
  same accident (no duplicate open/close events).
- flush(): force-close events and clear pending merges when the file/session ends.

Notes:
- Only processes accident streams; independent of weather/HLS/DB.
- Detection structure comes from events.bus:
  {type, camera_id, ts_unix, happened, confidence, frame_idx, pts_in_video}
"""
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

import time
from dataclasses import dataclass
from typing import Optional, Dict, Any

from events.bus import AsyncBus, Detection, topic_for


# Fixed Parameters
_ALPHA = 0.25                  # EMA smoothing coefficient
_EXIT_THR = 0.40               # Close-event EMA threshold (EMA must fall below this to allow closing)
_REQUIRED_HAPPENED_CONSEC = 3 # Strict open condition: must have N consecutive happened=True frames
_MIN_END_NEG_FRAMES = 8        # Close condition: at least N consecutive negative frames
                               # (driven by both EMA and happened)
_OCCLUSION_GRACE_SEC = 1.0     # Occlusion grace window: a pts_in_video gap larger than this counts as negative evolution
_MERGE_GAP_SEC = 5.0           # Merge window: if a new accident opens within this many seconds after closing,
                               # merge it into the previous accident

_TOPIC_IN_BASE    = "accident"         # Per-frame accident detection stream
_TOPIC_OPEN_BASE  = "accidents.open"   # Accident-open event topic base
_TOPIC_CLOSE_BASE = "accidents.close"  # Accident-close event topic base


@dataclass
class _Incident:
    id: str
    camera_id: str
    start_ts: float
    end_ts: float
    start_idx: int
    end_idx: int
    peak_conf: float = 0.0
    pos_frames: int = 0


class AccidentAggregator:
    """Partitioned-topic–based accident aggregator: focuses solely on accident detection results and is independent of detector implementation."""

    def __init__(self, camera_id: str, bus: AsyncBus, *, session_id: Optional[str] = None) -> None:
        self.camera_id = camera_id
        self.bus = bus
        self.session_id = session_id or str(int(time.time()))
        self._counter = 0

        # Aggregation state
        self.ema: float = 0.0
        self._hap_streak: int = 0
        self._neg_streak: int = 0
        self._open: Optional[_Incident] = None
        self._last_seen_pts: Optional[float] = None

        # Merge window: pending close event waiting for possible merging
        self._pending_close: Optional[Dict[str, Any]] = None   # Same structure as a close event
        self._pending_close_time: Optional[float] = None       # Timestamp of the last close event (measured in pts_in_video)


    # Utilities
    def _new_id(self) -> str:
        self._counter += 1
        return f"{self.session_id}-{self.camera_id}-{self._counter:06d}"

    async def _emit_open(self, inc: _Incident, det: Detection | None = None) -> None:
        ev = {

            "type": "accident_open",
            "camera_id": self.camera_id,
            "frame_idx": inc.start_idx,
            "pts_in_video": inc.start_ts,
            "confidence": inc.peak_conf,
            "session_id": self.session_id,
            "incident_id": inc.id,
            "peak_confidence": inc.peak_conf,
        }

        await self.bus.publish(topic_for(_TOPIC_OPEN_BASE, self.camera_id), ev)
        print(f" OPEN {ev}")

    async def _schedule_close(self, inc: _Incident, det: Detection | None = None) -> None:
        """Close the event and enter the merge observation window."""
        close_ev = {
            "type": "accident_close",
            "camera_id": self.camera_id,
            "frame_idx": inc.end_idx,
            "pts_in_video": inc.end_ts,
            "confidence": inc.peak_conf,
            "session_id": self.session_id,
            "incident_id": inc.id,
            "duration_sec": max(0.0, inc.end_ts - inc.start_ts),
            "peak_confidence": inc.peak_conf,
        }

        self._pending_close = close_ev
        self._pending_close_time = inc.end_ts
        print(f" CLOSE (pending merge) {close_ev}")

    async def _flush_pending_close_if_expired(self, now_pts: float) -> None:
        if self._pending_close is None:
            return
        if self._pending_close_time is None:
            return
        if now_pts - self._pending_close_time > _MERGE_GAP_SEC:

            await self.bus.publish(topic_for(_TOPIC_CLOSE_BASE, self.camera_id), self._pending_close)
            print(f"CLOSE (emit) {self._pending_close}")
            self._pending_close = None
            self._pending_close_time = None

    def _merge_reopen_into_pending(self, new_start_ts: float, new_end_ts: float, new_peak: float, new_pos_frames: int, new_end_idx: int) -> None:
        """Reopening within the merge window: merge the reopen into the pending close event, extending its duration and peak value."""
        assert self._pending_close is not None
        pc = self._pending_close
        pc["end_ts"] = new_end_ts
        pc["duration_sec"] = max(0.0, pc["end_ts"] - pc["start_ts"])
        pc["peak_confidence"] = max(float(pc["peak_confidence"]), float(new_peak))
        pc["pos_frames"] = int(pc.get("pos_frames", 0)) + int(new_pos_frames)
        # end_idx is only for logging/debugging (kept internally, not reported)

    # Main Loop
    async def run(self) -> None:
        topic_in = topic_for(_TOPIC_IN_BASE, self.camera_id)
        async with self.bus.subscribe(topic_in, mode="fifo", maxsize=128) as sub:
            while True:
                det: Detection = await sub.get()
                await self._process(det)

    async def _process(self, det: Detection) -> None:
        ts = float(getattr(det, "pts_in_video", 0.0))
        conf = float(getattr(det, "confidence", 0.0))
        happened = bool(getattr(det, "happened", False))
        fidx = int(getattr(det, "frame_idx", 0))

        # First handle timeout publication of the pending close event
        await self._flush_pending_close_if_expired(ts)

        # Compute occlusion / frame gap
        prev_pts = self._last_seen_pts
        self._last_seen_pts = ts
        occlusion_ok = True
        if prev_pts is not None and (ts - prev_pts) > _OCCLUSION_GRACE_SEC:
            # Gap exceeds grace threshold; treat it as a discontinuity and handle close-condition more conservatively
            occlusion_ok = False

        # EMA smoothing
        self.ema = _ALPHA * conf + (1.0 - _ALPHA) * self.ema

        # Consecutive positive streak (used for “strict open” condition)
        if happened:
            self._hap_streak += 1
        else:
            self._hap_streak = 0

        # Negative-evolution counter (increments only when EMA is below threshold; does not increment during occlusion anomalies)
        if self.ema <= _EXIT_THR and occlusion_ok:
            self._neg_streak += 1
        else:
            self._neg_streak = 0

        # Open Event Decision
        if self._open is None and self._hap_streak >= _REQUIRED_HAPPENED_CONSEC:
            # If a pending close exists and we are still within the merge window → merge the reopen
            if self._pending_close is not None and self._pending_close_time is not None:
                if (ts - self._pending_close_time) <= _MERGE_GAP_SEC:
                    # Merge the reopen into the previous accident: update the pending close's end_ts / peak / pos_frames
                    new_peak = conf
                    new_pos = 1  # Count this frame as a positive frame

                    self._merge_reopen_into_pending(
                        new_start_ts=ts, new_end_ts=ts, new_peak=new_peak, new_pos_frames=new_pos, new_end_idx=fidx
                    )
                    # After merging, treat the accident as still ongoing: restore the open state
                    inc = _Incident(
                        id=self._pending_close["incident_id"],
                        camera_id=self.camera_id,
                        start_ts=self._pending_close["start_ts"],
                        end_ts=ts,
                        start_idx=int(getattr(det, "frame_idx", fidx)),
                        end_idx=fidx,
                        peak_conf=float(self._pending_close["peak_confidence"]),
                        pos_frames=int(self._pending_close.get("pos_frames", 0)),
                    )
                    # Clear the pending close and restore the open state
                    self._pending_close = None
                    self._pending_close_time = None
                    self._open = inc
                    self._hap_streak = 0
                    return

            # Normal new accident opening
            inc = _Incident(
                id=self._new_id(),
                camera_id=self.camera_id,
                start_ts=ts,
                end_ts=ts,
                start_idx=fidx,
                end_idx=fidx,
                peak_conf=conf,
                pos_frames=1,
            )
            self._open = inc
            self._hap_streak = 0
            await self._emit_open(inc)
            return

        # Ongoing Event Update
        if self._open is not None:
            inc = self._open
            inc.end_ts = ts
            inc.end_idx = fidx
            inc.peak_conf = max(inc.peak_conf, conf)
            if happened:
                inc.pos_frames += 1

            # Close-event decision: EMA stays below threshold + consecutive negative frames reach the minimum requirement
            if self.ema <= _EXIT_THR and self._neg_streak >= _MIN_END_NEG_FRAMES:
                # Enter the merge window: do not publish immediately; wait _MERGE_GAP_SEC to capture possible re-openings
                await self._schedule_close(inc)
                self._open = None
                self.ema = 0.0
                self._neg_streak = 0

    # flush
    async def flush(self) -> None:
        """
        When the video/session ends:
        - If an accident is still open: immediately generate and publish a close event (no merge).
        - If a pending close exists: publish it immediately and clear it.
        """

        did_close = False

        # If a pending close still exists → publish it and clear the state
        if self._pending_close is not None:
            await self.bus.publish(topic_for(_TOPIC_CLOSE_BASE, self.camera_id), self._pending_close)
            print(f" CLOSE (emit pending) {self._pending_close}")
            self._pending_close = None
            self._pending_close_time = None
            did_close = True

        # If an accident is still open → force a close
        if self._open is not None:
            inc = self._open
            ev = {
                "type": "accident_close",
                "session_id": self.session_id,
                "incident_id": inc.id,
                "camera_id": self.camera_id,
                "start_ts": inc.start_ts,
                "end_ts": inc.end_ts,
                "duration_sec": max(0.0, inc.end_ts - inc.start_ts),
                "peak_confidence": inc.peak_conf,
                "pos_frames": inc.pos_frames,
                "reason": "flush_open",
            }
            await self.bus.publish(topic_for(_TOPIC_CLOSE_BASE, self.camera_id), ev)
            print(f" [Aggregator] flush_close {ev}")
            self._open = None
            did_close = True

        # If neither exists
        if not did_close:
            print(f"[Aggregator] flush(): 无需结案。")
