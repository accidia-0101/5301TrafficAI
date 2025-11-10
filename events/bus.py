"""
简洁稳定的异步事件总线（AsyncBus）
- 分区主题：topic_for("frames", "cam-1") -> "frames:cam-1"
- 订阅：async with bus.subscribe(topic, mode="fifo"|"latest", maxsize=64) as q: item = await q.get()
- 发布：await bus.publish(topic, item)
- 分区发布：await bus.publish_partitioned(base, camera_id, item)
- 特点：发布端永不阻塞；慢订阅者丢旧保新；订阅退出自动清理
"""
from dataclasses import dataclass
from typing import Any, Optional, Dict, List
import asyncio
from contextlib import asynccontextmanager
import numpy as np  # 仅用于类型注解

# ---------- 分区主题工具 ----------
def topic_for(base: str, camera_id: Optional[str] = None) -> str:
    """构造分区主题名，如 'frames_raw:cam-1'。"""
    return f"{base}:{camera_id}" if camera_id else base


# ---------- 数据结构 ----------
@dataclass(slots=True)
class Frame:
    camera_id: str
    ts_unix: float
    rgb: np.ndarray
    frame_idx: int = 0
    pts_in_video: float = 0.0


@dataclass(slots=True)
class Detection:
    type: str
    camera_id: str
    ts_unix: float
    happened: bool
    confidence: float
    frame_idx: int = 0
    pts_in_video: float = 0.0


# ---------- 订阅端 ----------
class _Subscriber:
    __slots__ = ("queue", "mode")

    def __init__(self, mode: str, maxsize: int):
        self.mode = "latest" if mode == "latest" else "fifo"
        cap = 1 if self.mode == "latest" else max(1, int(maxsize))
        self.queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=cap)

    def deliver(self, item: Any) -> None:
        if self.mode == "latest":
            if self.queue.full():
                try:
                    self.queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
            try:
                self.queue.put_nowait(item)
            except asyncio.QueueFull:
                try:
                    while True:
                        self.queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
                try:
                    self.queue.put_nowait(item)
                except asyncio.QueueFull:
                    pass
        else:
            try:
                self.queue.put_nowait(item)
            except asyncio.QueueFull:
                try:
                    self.queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
                try:
                    self.queue.put_nowait(item)
                except asyncio.QueueFull:
                    pass


# ---------- 异步事件总线 ----------
class AsyncBus:
    """
    简单稳定的 Pub/Sub：
      - subscribe(topic, mode='fifo'|'latest', maxsize=64)
      - publish(topic, item) 非阻塞
      - publish_partitioned(base, camera_id, item)
      - subscribe_many(topics) 合并订阅多个主题
    """
    __slots__ = ("_topics", "_lock")

    def __init__(self):
        self._topics: Dict[str, List[_Subscriber]] = {}
        self._lock = asyncio.Lock()

    # 单主题订阅
    @asynccontextmanager
    async def subscribe(self, topic: str, *, mode: str = "fifo", maxsize: int = 64):
        sub = _Subscriber(mode=mode, maxsize=maxsize)
        async with self._lock:
            self._topics.setdefault(topic, []).append(sub)
            print(f"[bus] subscribe -> {topic}")
        try:
            yield sub.queue
        finally:
            async with self._lock:
                lst = self._topics.get(topic)
                if lst is not None:
                    try:
                        lst.remove(sub)
                    except ValueError:
                        pass
                    if not lst:
                        self._topics.pop(topic, None)
            print(f"[bus] unsubscribe -> {topic}")

    # 多主题订阅（合并）
    @asynccontextmanager
    async def subscribe_many(self, topics: List[str], *, mode: str = "fifo", maxsize: int = 64):
        """一次订阅多个主题并合并输出队列"""
        subs: List[_Subscriber] = []
        merged_q: asyncio.Queue = asyncio.Queue(maxsize=maxsize)

        async def forward(sub: _Subscriber):
            while True:
                item = await sub.queue.get()
                await merged_q.put(item)

        async with self._lock:
            for t in topics:
                s = _Subscriber(mode=mode, maxsize=maxsize)
                self._topics.setdefault(t, []).append(s)
                subs.append(s)
                print(f"[bus] subscribe_many -> {t}")
                asyncio.create_task(forward(s))

        try:
            yield merged_q
        finally:
            async with self._lock:
                for t, s in zip(topics, subs):
                    lst = self._topics.get(t)
                    if lst and s in lst:
                        lst.remove(s)
                    if lst == []:
                        self._topics.pop(t, None)
                    print(f"[bus] unsubscribe_many -> {t}")

    # 发布
    async def publish(self, topic: str, item: Any) -> None:
        async with self._lock:
            subs = list(self._topics.get(topic, []))
        if not subs:
            return
        for sub in subs:
            try:
                sub.deliver(item)
            except Exception:
                continue
        # ✅ 日志可选
        # print(f"[bus.publish] {topic}")

    async def publish_partitioned(self, base: str, camera_id: str, item: Any) -> None:
        await self.publish(topic_for(base, camera_id), item)

    async def close_topic(self, topic: str) -> None:
        async with self._lock:
            self._topics.pop(topic, None)
