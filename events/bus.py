# events/bus.py
from dataclasses import dataclass
from typing import Any, Optional, Dict, List, Tuple
import numpy as np, asyncio
from contextlib import asynccontextmanager

# ---------- 数据结构 ----------
@dataclass
class Frame:
    camera_id: str
    ts_unix: float
    rgb: np.ndarray               # HxWx3, uint8, RGB
    frame_idx: int = 0            # 帧号（从 0 递增）
    pts_in_video: float = 0.0     # 相对视频起点秒

@dataclass
class Detection:
    type: str                     # accident/weather/...
    camera_id: str
    ts_unix: float
    happened: bool
    confidence: float
    # 补这两项，保证和 HLS 时间轴对齐（可选）
    frame_idx: int = 0
    pts_in_video: float = 0.0

# ---------- 订阅端适配器 ----------
class _Subscriber:
    __slots__ = ("queue", "mode")
    def __init__(self, mode: str, maxsize: int):
        self.mode = mode  # "fifo" or "latest"
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=maxsize)

    async def deliver(self, item: Any):
        if self.mode == "latest":
            # 保证“最新优先”：满了就丢掉最旧的，再放新
            if self.queue.full():
                try:
                    self.queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
            try:
                self.queue.put_nowait(item)
            except asyncio.QueueFull:
                # 极端并发下再次满，直接覆盖：先清，再放
                try:
                    while not self.queue.empty():
                        self.queue.get_nowait()
                except Exception:
                    pass
                await self.queue.put(item)
        else:
            # FIFO：尽量不丢帧；但不阻塞 publisher
            try:
                self.queue.put_nowait(item)
            except asyncio.QueueFull:
                # 保守策略：丢最旧，腾位放新，防“全链路被一个慢订阅者拖死”
                try:
                    self.queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
                await self.queue.put(item)

# ---------- 异步总线 ----------
class AsyncBus:
    def __init__(self):
        self._topics: Dict[str, List[_Subscriber]] = {}
        self._lock = asyncio.Lock()

    @asynccontextmanager
    async def subscribe(self, topic: str, *, mode: str = "fifo", maxsize: int = 64):
        """
        mode = "fifo"  ：分析/采样等需要按序处理的消费者
        mode = "latest": 推流/预览等只关心最新帧的消费者（典型：HLS/MJPEG）
        """
        sub = _Subscriber(mode=mode, maxsize=maxsize if mode == "fifo" else 1)
        async with self._lock:
            self._topics.setdefault(topic, []).append(sub)
        try:
            yield sub.queue
        finally:
            async with self._lock:
                lst = self._topics.get(topic, [])
                if sub in lst:
                    lst.remove(sub)

    def subscribe_legacy(self, topic: str, *, mode: str = "fifo", maxsize: int = 64) -> asyncio.Queue:
        """
        兼容你现有的 q = bus.subscribe("frames") 写法：
        返回 queue，但内部也注册了取消逻辑（需要调用者不关心）。
        """
        # 用 asynccontextmanager 包一层同步取出 queue（不推荐新代码用）
        loop = asyncio.get_event_loop()
        q_holder: Tuple[asyncio.Queue] = (None,)  # trick: 可变闭包
        async def _enter():
            cm = self.subscribe(topic, mode=mode, maxsize=maxsize)
            q = await cm.__aenter__()
            q_holder_list.append(cm)
            return q
        q_holder_list = []
        q = loop.run_until_complete(_enter())
        return q

    async def publish(self, topic: str, item: Any):
        # 拷贝列表，避免发布过程中订阅变更影响遍历
        async with self._lock:
            subs = list(self._topics.get(topic, []))
        if not subs:
            return
        # 逐个投递；不 await gather，避免短时间创建过多任务影响调度
        for sub in subs:
            try:
                await sub.deliver(item)
            except Exception:
                # 单个订阅者异常不影响其他订阅者
                pass
