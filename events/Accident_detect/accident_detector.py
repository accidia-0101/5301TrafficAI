# detector_accident.py
"""
YOLOv8 单类事故检测（固定使用训练好的 best.pt）

功能：
- 订阅 'frames'（Frame.rgb: HxWx3, uint8, RGB）
- 对每一帧做 best 推理（nc=1, names:['accident']）
- 若检测到任意框：帧置信度 = max(boxes.conf)
- happened = (frame_conf >= decision_thresh)
- 发布到 'detections' 主题
"""

from __future__ import annotations
import asyncio
from typing import Optional
import numpy as np
from events.bus import Frame, Detection, AsyncBus


# 权重路径（可改成绝对路径）
MODEL_PATH = r"E:\PythonProject\DjangoTrafficAI\events\pts\best.pt"


class AccidentDetector:
    """YOLOv8 单类事故检测引擎"""
    def __init__(
        self,
        *,
        imageSize: int = 960,
        yolo_conf: float = 0.05,
        yolo_iou: float = 0.50,
        device: Optional[str | int] = 0,  # 默认用 GPU 0，可改为 "cpu"
    ):
        try:
            from ultralytics import YOLO
        except Exception as e:
            raise RuntimeError("缺少 ultralytics，请先执行 `pip install ultralytics`") from e

        self.imageSize = imageSize
        self.yolo_conf = yolo_conf
        self.yolo_iou = yolo_iou
        self.device = device

        print(f"🔹 正在加载模型权重: {MODEL_PATH}")
        self._yolo = YOLO(MODEL_PATH)

        if hasattr(self._yolo, "overrides"):
            self._yolo.overrides["conf"] = yolo_conf
            self._yolo.overrides["iou"] = yolo_iou
            self._yolo.overrides["device"] = device

    def infer_frame_conf(self, rgb: np.ndarray) -> float:
        """
        对单帧推理并返回帧级置信度：
          - 无框：0.0
          - 有框：max(boxes.conf)
        """
        res = self._yolo.predict(
            rgb,
            imgsz=self.imageSize,
            conf=self.yolo_conf,
            iou=self.yolo_iou,
            verbose=False,
            device=self.device,
        )[0]

        boxes = getattr(res, "boxes", None)
        if boxes is None or len(boxes) == 0:
            return 0.0

        confs = boxes.conf
        if confs is None or len(confs) == 0:
            return 0.0

        return float(confs.max().item())


async def run_accident_detector(
    bus: AsyncBus,
    *,
    decision_thresh: float = 0.65,  # 帧级判定阈值
    imgsz: int = 960,
    yolo_conf: float = 0.05,
    yolo_iou: float = 0.50,
    device: Optional[str | int] = 0,
):
    """
    订阅 'frames' → 对每一帧推理 → 发布 'detections'
    """
    q = bus.subscribe("frames")
    engine = AccidentDetector(
        imageSize=imgsz,
        yolo_conf=yolo_conf,
        yolo_iou=yolo_iou,
        device=device,
    )

    while True:
        frame: Frame = await q.get()
        frame_conf = engine.infer_frame_conf(frame.rgb)
        happened = frame_conf >= decision_thresh

        det = Detection(
            type="accident",
            camera_id=frame.camera_id,
            ts_unix=frame.ts_unix,
            happened=happened,
            confidence=frame_conf,
        )
        await bus.publish("detections", det)
        await asyncio.sleep(0)
