# -*- coding: utf-8 -*-
"""
运行时全局状态（逻辑分区 + 单实例多路检测模式）
"""
from events.bus import AsyncBus
import asyncio, threading

# ---------- 全局运行状态 ----------
BUS = AsyncBus()          # 全局唯一事件总线
SESSIONS = {}             # camera_id -> SingleFileSession 实例
INTENDED = {}             # camera_id -> 视频源路径
DETECTOR_TASK = None      # asyncio.Task：全局 run_accident_detector_multi 任务
SSE_PENDING = {}          # sse_id -> List[camera_id]

# ---------- 后台事件循环 ----------
_BG_LOOP = None
_BG_THREAD = None
_BG_LOCK = threading.Lock()

def ensure_bg_loop():
    """确保全局后台事件循环存活"""
    global _BG_LOOP, _BG_THREAD
    with _BG_LOCK:
        if _BG_LOOP is None or _BG_LOOP.is_closed():
            loop = asyncio.new_event_loop()
            def runner():
                asyncio.set_event_loop(loop)
                loop.run_forever()
            t = threading.Thread(target=runner, name="trafficai-bg-loop", daemon=True)
            t.start()
            _BG_LOOP, _BG_THREAD = loop, t
    return _BG_LOOP

def stop_bg_loop():
    """（可选）在系统退出时停止后台事件循环"""
    global _BG_LOOP
    if _BG_LOOP and not _BG_LOOP.is_closed():
        _BG_LOOP.call_soon_threadsafe(_BG_LOOP.stop)
