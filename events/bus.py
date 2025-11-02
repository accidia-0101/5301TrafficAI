# 总线分发帧
from dataclasses import dataclass
import numpy as np
import asyncio

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
        if not qs: return
        # 并发 put；单个订阅者阻塞不拖累其他订阅者
        async def _safe_put(queue: asyncio.Queue):
            try:
                await asyncio.wait_for(queue.put(item), timeout=0.1)  # 100ms 容忍
            except asyncio.TimeoutError:
                # 丢弃本次投递，避免背压；也可改成 await queue.put(item) 强背压
                pass
        await asyncio.gather(*[_safe_put(q) for q in qs])
