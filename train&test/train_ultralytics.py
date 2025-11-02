""""
Accident and Non-accident label Image Dataset > Hai-s Augment attempt
https://universe.roboflow.com/accident-and-nonaccident/accident-and-non-accident-label-image-dataset
Provided by a Roboflow user
License: Public Domain
"""

from pathlib import Path
import os, platform, torch, yaml, glob
from ultralytics import YOLO

# ---------- 根据 CPU/OS 选 workers ----------
def pick_workers():
    cpu = os.cpu_count() or 4
    sys = platform.system().lower()
    return (min(4, max(2, cpu // 4)) if sys.startswith("win")
            else min(16, max(4, cpu // 2)))

# ---------- 小工具：解析出 images/labels 的绝对路径并做存在性检查 ----------
def resolve_split_dirs(data_yaml_path: Path, rel_images_dir: str):
    # data.yaml 的相对路径是相对于 data.yaml 所在目录
    abs_images = (data_yaml_path.parent / Path(rel_images_dir)).resolve()
    # YOLO 的标签目录与 images 平行，把 images 替换为 labels
    if abs_images.name.lower() == "images":
        abs_labels = abs_images.parent / "labels"
    else:
        # 如果不是标准命名，仍尝试 images->labels 的替换
        abs_labels = Path(str(abs_images).replace(os.sep + "images", os.sep + "labels"))
    return abs_images, abs_labels

def quick_class_count(labels_dir: Path, num_classes: int = 2):
    """快速统计某个 labels 目录下各类别出现次数（用于发现 'non-accident' 是否有框）"""
    counts = [0] * num_classes
    if not labels_dir.exists():
        return counts
    for txt in glob.glob(str(labels_dir / "*.txt")):
        try:
            with open(txt, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    cid = int(line.split()[0])
                    if 0 <= cid < num_classes:
                        counts[cid] += 1
        except Exception:
            pass
    return counts

def main():
    # ===== 1) 定位你的 data.yaml（项目外路径，已按你的反馈写死）=====
    data_yaml = Path(r"E:\Training\CCD-DATA\data.yaml").resolve()
    assert data_yaml.exists(), f"找不到 data.yaml：{data_yaml}"

    print("===> PyTorch:", torch.__version__)
    print("===> CUDA 可用:", torch.cuda.is_available())
    if torch.cuda.is_available():
        print("===> GPU:", torch.cuda.get_device_name(0))

    # ===== 2) 读取 data.yaml，打印关键字段并解析出绝对路径 =====
    with open(data_yaml, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    train_rel = cfg.get("train")
    val_rel   = cfg.get("val") or cfg.get("valid")
    test_rel  = cfg.get("test")
    nc        = int(cfg.get("nc", 1))
    names     = cfg.get("names")

    assert train_rel and val_rel, "data.yaml 必须包含 train 与 val"
    print("===> data.yaml 解析：")
    print("nc:", nc, "names:", names)

    train_imgs, train_lbls = resolve_split_dirs(data_yaml, train_rel)
    val_imgs,   val_lbls   = resolve_split_dirs(data_yaml, val_rel)
    test_imgs,  test_lbls  = (resolve_split_dirs(data_yaml, test_rel) if test_rel else (None, None))

    print("train images:", train_imgs)
    print("train labels:", train_lbls)
    print("val   images:", val_imgs)
    print("val   labels:", val_lbls)
    if test_imgs:
        print("test  images:", test_imgs)
        print("test  labels:", test_lbls)

    # 目录存在性提示（不强制中断，避免硬失败）
    for p in [train_imgs, train_lbls, val_imgs, val_lbls] + ([test_imgs, test_lbls] if test_imgs else []):
        if p and not p.exists():
            print(f"⚠️  警告：目录不存在：{p}")

    # 快速统计各 split 的类别出现频次（帮助你发现 'non-accident' 是否真的有框）
    if nc >= 2:
        tr_cnt = quick_class_count(train_lbls, nc)
        va_cnt = quick_class_count(val_lbls, nc)
        print(f"===> 训练集标签类别计数：{tr_cnt}  (索引对应 names)")
        print(f"===> 验证集标签类别计数：{va_cnt}")
        if sum(tr_cnt[1:]) == 0 and sum(va_cnt[1:]) == 0:
            print("⚠️  提示：检测到除第0类外其他类别几乎没有框。"
                  "若 'non-accident' 只是负样本（没有框），更标准的做法是改为 nc:1 和 names:['accident']，"
                  "并保持非事故图片对应的 labels/*.txt 为空。")

    # ===== 3) 载入 YOLO 权重（为你的 16GB 显存优化：yolov8m + 640）=====
    model = YOLO("../events/pts/yolov8m.pt")

    workers = pick_workers()
    print("===> workers:", workers)

    train_args = dict(
        data=str(data_yaml),          # YOLO 会自己按相对路径解析 split
        epochs=100,
        imgsz=640,                    # 16GB 显存推荐960；紧张可降 896/640
        batch=16,                 # 让 YOLO 根据显存自动选择 batch
        device=0 if torch.cuda.is_available() else "cpu",
        workers=workers,
        patience=50,
        optimizer="auto",
        lr0=0.005,
        cos_lr=True,
        cache="ram",                  # 48GB 内存可开，加速 IO
        pretrained=True,
        amp=True,
        mosaic=0.2,
        hsv_h=0.015, hsv_s=0.7, hsv_v=0.4,
        translate=0.1, scale=0.5, shear=0.0, flipud=0.0, fliplr=0.5,
        val=True,
        project="runs_accident",
        name="yolov8m_accident_640",
        exist_ok=True,
    )

    print("===> 开始训练：", {k: v for k, v in train_args.items() if k not in ("optimizer",)})
    results = model.train(**train_args)

    # ===== 4) 训练后验证 =====
    print("===> 验证中 ...")
    metrics = model.val(data=str(data_yaml),
                        imgsz=train_args["imgsz"],
                        device=train_args["device"])
    print("===> 验证指标：", metrics)

    # ===== 5) 如有 test，评估 test =====
    if test_rel:
        print("===> 在 test 集上评估 ...")
        model.val(data=str(data_yaml),
                  split="test",
                  imgsz=train_args["imgsz"],
                  device=train_args["device"])

    # ===== 6) 导出 ONNX（基于 best.pt）=====
    # Ultralytics 训练输出目录：{project}/{name}/weights/best.pt
    save_dir = Path(getattr(results, "save_dir", Path(train_args["project"]) / train_args["name"]))
    best_pt = save_dir / "weights" / "best.pt"
    if best_pt.exists():
        print(f"===> 导出 ONNX（来源：{best_pt}） ...")
        YOLO(str(best_pt)).export(format="onnx")
    else:
        print("⚠️  未找到 best.pt，改为导出当前模型权重（可能不是最佳）")
        model.export(format="onnx")

    print("✅ 全流程完成。输出目录：", save_dir)

if __name__ == "__main__":
    main()
