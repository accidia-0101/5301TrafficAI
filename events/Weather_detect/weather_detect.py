# # weather_module.py
# import time, queue, threading, torch, torch.nn as nn
# from torchvision import models, transforms
# from datetime import datetime, timezone, timedelta
# from collections import deque
# import numpy as np
#
# CLASSES = ["clear", "rain", "fog"]
# CONF_TH = 0.55            # 置信度阈值
# SMOOTH_SEC = 90           # 多数票窗口秒数
# COMMIT_INTERVAL = 300     # 5分钟 = 300s
#
# class WeatherNet(nn.Module):
#     def __init__(self, num_classes=3):
#         super().__init__()
#         self.backbone = models.mobilenet_v3_small(weights=models.MobileNet_V3_Small_Weights.DEFAULT)
#         in_feats = self.backbone.classifier[-1].in_features
#         self.backbone.classifier[-1] = nn.Linear(in_feats, num_classes)
#     def forward(self, x): return self.backbone(x)
#
# preprocess = transforms.Compose([
#     transforms.ToTensor(),
#     transforms.Resize((224,224), antialias=True),
#     transforms.Normalize(mean=[0.485,0.456,0.406], std=[0.229,0.224,0.225]),
# ])
#
# @torch.inference_mode()
# def predict_batch(model, frames):  # frames: list[np.ndarray HxWx3 BGR/RGB->需转RGB]
#     # 假设传入的是RGB uint8
#     xs = torch.stack([preprocess(torch.from_numpy(f).permute(2,0,1).float()/255.0) for f in frames], dim=0)
#     xs = xs.cuda().half()
#     logits = model(xs)
#     probs = logits.softmax(dim=1).float().cpu().numpy()
#     return probs  # shape [B,3]
#
# def majority_vote(buffer):
#     # buffer: deque of (label_idx, conf, ts)
#     if not buffer: return None, 0.0
#     labels = [l for (l, c, t) in buffer]
#     counts = np.bincount(labels, minlength=3)
#     winner = int(np.argmax(counts))
#     # 取该类的平均置信度作参考
#     confs = [c for (l, c, t) in buffer if l == winner]
#     return winner, float(np.mean(confs)) if confs else 0.0
#
# def persist_weather_event(camera_id, label, conf):
#     # TODO: 用 Django ORM 写入 events(type='weather', weather=label, confidence=conf, timestamp=utc_now, camera_id=...)
#     # 并通过 WebSocket 群发
#     pass
#
# class WeatherEngine:
#     def __init__(self, camera_id: str, weights_path: str):
#         self.model = WeatherNet().cuda().eval().half()
#         self.model.load_state_dict(torch.load(weights_path, map_location="cuda"))
#         self.camera_id = camera_id
#         self.buf = deque()
#         self.last_commit_ts = 0.0
#         self.last_committed_label = None
#
#     def run(self, frame_iterable):
#         """
#         frame_iterable: 产生 RGB 帧的可迭代对象（建议按 1~2 FPS 抽样）
#         """
#         batch = []
#         batch_ts = []
#         for frame in frame_iterable:
#             batch.append(frame)
#             batch_ts.append(time.time())
#             if len(batch) < 16:   # 小批次
#                 continue
#             probs = predict_batch(self.model, batch)
#             for p, ts in zip(probs, batch_ts):
#                 label_idx = int(np.argmax(p))
#                 conf = float(np.max(p))
#                 # 仅将最近 SMOOTH_SEC 的样本保留
#                 now = time.time()
#                 self.buf.append((label_idx, conf, ts))
#                 while self.buf and (now - self.buf[0][2] > SMOOTH_SEC):
#                     self.buf.popleft()
#             # 清空批
#             batch.clear(); batch_ts.clear()
#
#             # 平滑后的输出
#             winner, avg_conf = majority_vote(self.buf)
#             if winner is None: continue
#             if avg_conf < CONF_TH:  # 低置信度则不更新
#                 continue
#
#             # 5分钟节流 + 仅状态变更写库
#             now = time.time()
#             label_name = CLASSES[winner]
#             if (self.last_committed_label != label_name) and (now - self.last_commit_ts >= COMMIT_INTERVAL):
#                 persist_weather_event(self.camera_id, label_name, round(avg_conf, 3))
#                 self.last_commit_ts = now
#                 self.last_committed_label = label_name
