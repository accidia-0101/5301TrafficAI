import asyncio
from pprint import pformat

from events.bus import AsyncBus
from events.frame_discrete import run_frame_source
from events.Accident_detect.accident_detector import AccidentDetector

ACC_WEIGHTS = r"E:\PythonProject\DjangoTrafficAI\events\pts\best.pt"
VIDEO_PATH  = r"E:\Training\Recording 2025-10-30 172929.mp4"
CAMERA_ID   = "Camera-01"

# ------- 1) 给总线加 Tap：打印所有 publish 的主题与摘要 -------
class TappedBus:
    """
    包装你的 AsyncBus：拦截所有 publish 打印出来，便于定位真正的主题名与消息结构。
    其余接口透传。
    """
    def __init__(self, inner: AsyncBus, preview_len: int = 120):
        self.inner = inner
        self.preview_len = preview_len

    def subscribe(self, topic: str):
        return self.inner.subscribe(topic)

    async def publish(self, topic: str, msg):
        # 打印摘要（避免刷屏）
        summary = repr(msg)
        if len(summary) > self.preview_len:
            summary = summary[: self.preview_len] + "..."
        print(f"[BUS] publish -> {topic}: {summary}")
        await self.inner.publish(topic, msg)

# ------- 2) 一个“多主题嗅探器”：只要像事故就打印 -------
LIKELY_TOPICS = ["accident", "detections", "events", "accident:confirmed", "incident"]

def looks_like_accident(x) -> tuple[bool, str]:
    """
    尝试用多种启发判断“这是不是事故事件”，并返回(是否事故, 说明)。
    你可以根据实际结构再加规则。
    """
    try:
        if isinstance(x, dict):
            if x.get("happened") is True:
                return True, "happened=True"
            if x.get("type") in {"accident", "incident"}:
                return True, f"type={x.get('type')!r}"
            # 一些常见字段组合
            keys = set(k.lower() for k in x.keys())
            if "accident" in keys:
                v = x.get("accident")
                if isinstance(v, (bool, int)) and bool(v):
                    return True, "key 'accident' truthy"
            if "label" in keys and str(x.get("label")).lower() in {"accident", "crash"}:
                return True, "label indicates accident"
        # 其他简单可能：字符串里带 accident
        if isinstance(x, str) and "accident" in x.lower():
            return True, "string contains 'accident'"
    except Exception:
        pass
    return False, ""

async def multi_topic_print_sink(bus: AsyncBus):
    queues = {topic: bus.subscribe(topic) for topic in LIKELY_TOPICS}
    print(f"[sink] 已订阅：{', '.join(LIKELY_TOPICS)}")

    # 把多个队列合并等候
    async def consume(topic, q):
        while True:
            msg = await q.get()
            is_acc, why = looks_like_accident(msg)
            if is_acc:
                cam = (msg.get("camera_id") if isinstance(msg, dict) else None) or "?"
                conf = (msg.get("confidence") if isinstance(msg, dict) else None)
                conf_str = f"{conf:.2f}" if isinstance(conf, (int, float)) else "n/a"
                print(f"[ACCIDENT★] topic={topic} camera={cam} confidence={conf_str} why={why}")
                # 同时把完整消息结构打印一次（便于你接下来精确匹配）
                print("[ACCIDENT★] full msg =", pformat(msg))
            q.task_done()

    # 并发消费所有订阅主题
    consumers = [asyncio.create_task(consume(t, q)) for t, q in queues.items()]
    await asyncio.gather(*consumers)

# ------- 3) 主流程 -------
async def main():
    raw_bus = AsyncBus()
    bus = TappedBus(raw_bus)  # 用带打印的 bus

    # A: 视频帧源（注意：你的 run_frame_source 必须往检测器期望的主题发布帧）
    task_frames = asyncio.create_task(
        run_frame_source(
            bus,
            camera_id=CAMERA_ID,
            url_or_path=VIDEO_PATH,
            target_fps=15,
        )
    )

    # B: 事故检测
    acc = AccidentDetector(
        weights_path=ACC_WEIGHTS,
        conf_th=0.40, iou_th=0.50, imageSize=960,
        sample_every_frames=10,
        min_infer_interval_sec=0.10,
    )
    task_acc = asyncio.create_task(acc.run(bus))

    # C: 嗅探/打印
    task_sink = asyncio.create_task(multi_topic_print_sink(raw_bus))

    await asyncio.gather(task_frames, task_acc, task_sink)

if __name__ == "__main__":
    asyncio.run(main())
