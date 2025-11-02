from ultralytics import YOLO
import torch

model = YOLO(r"E:\PythonProject\DjangoTrafficAI\events\pts\best.pt")
print("CUDA:", torch.cuda.is_available())
# 预测整夹图像，结果会存到 runs\detect\predictX\
model.predict(
    source=r"E:\Training\crash.png",  # 可是单图/文件夹/通配符/视频路径/rtsp
    imgsz=640,
    conf=0.25,     # 置信阈值
    iou=0.5,       # NMS IoU
    device=0,      # 无GPU就删掉此行
    save=True,     # 保存可视化框图
    save_txt=True, # 保存YOLO格式txt
)
