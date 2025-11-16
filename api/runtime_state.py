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

"""
Global runtime state (logical partitioning + single-instance multi-stream detection mode)
"""


from events.bus import AsyncBus
import asyncio
import threading

BUS = AsyncBus()               # Global singleton event bus
SESSIONS = {}                  # camera_id -> SingleFileSession instance
INTENDED = {}                  # camera_id -> video source path

DETECTOR_TASK = None           # asyncio.Task: run_accident_detector_multi task
WEATHER_TASK = None            # asyncio.Task: run_weather_detector_multi task

SSE_PENDING = {}               # sse_id -> [camera_id]

# ★ Latest weather cache: updated by the weather detector, used to supplement weather info for accident_open events
LAST_WEATHER = {}              # { camera_id: "clear"/"rain"/"fog" }

# ★ Background storage worker handle: started by SessionManager.stream()
SAVE_WORKER = None             # concurrent.futures.Future / asyncio.Future


_BG_LOOP = None
_BG_THREAD = None
_BG_LOCK = threading.Lock()


def ensure_bg_loop():
    """Ensure that the global background event loop is running (used for run_coroutine_threadsafe)."""
    global _BG_LOOP, _BG_THREAD
    with _BG_LOCK:
        if _BG_LOOP is None or _BG_LOOP.is_closed():
            loop = asyncio.new_event_loop()

            def runner():
                asyncio.set_event_loop(loop)
                loop.run_forever()

            t = threading.Thread(
                target=runner,
                name="trafficai-bg-loop",
                daemon=True,
            )
            t.start()

            _BG_LOOP, _BG_THREAD = loop, t

    return _BG_LOOP


def stop_bg_loop():
    """Optional: stop the background event loop when the system shuts down.Can be used in the future"""
    global _BG_LOOP
    if _BG_LOOP and not _BG_LOOP.is_closed():
        _BG_LOOP.call_soon_threadsafe(_BG_LOOP.stop)
