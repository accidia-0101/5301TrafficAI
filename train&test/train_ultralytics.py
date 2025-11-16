""""
Accident and Non-accident label Image Dataset > Hai-s Augment attempt
https://universe.roboflow.com/accident-and-nonaccident/accident-and-non-accident-label-image-dataset
Provided by a Roboflow user
License: Public Domain
"""
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
from pathlib import Path
import os, platform, torch, yaml, glob
from ultralytics import YOLO

# Select workers based on CPU/OS
def pick_workers():
    cpu = os.cpu_count() or 4
    sys = platform.system().lower()
    return (min(4, max(2, cpu // 4)) if sys.startswith("win")
            else min(16, max(4, cpu // 2)))

# Utility: resolve absolute paths for images/labels and check existence
def resolve_split_dirs(data_yaml_path: Path, rel_images_dir: str):
    abs_images = (data_yaml_path.parent / Path(rel_images_dir)).resolve()
    if abs_images.name.lower() == "images":
        abs_labels = abs_images.parent / "labels"
    else:
        abs_labels = Path(str(abs_images).replace(os.sep + "images", os.sep + "labels"))
    return abs_images, abs_labels

def quick_class_count(labels_dir: Path, num_classes: int = 2):
    """Quickly count occurrences of each class under a labels directory (useful for checking whether 'non-accident' has bounding boxes)."""
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
    data_yaml = Path(r"E:\Training\CCD-DATA\data.yaml").resolve()
    assert data_yaml.exists(), f"can not find data.yaml：{data_yaml}"
    print("===> PyTorch:", torch.__version__)
    print("===> CUDA :", torch.cuda.is_available())
    if torch.cuda.is_available():
        print("===> GPU:", torch.cuda.get_device_name(0))

    with open(data_yaml, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    train_rel = cfg.get("train")
    val_rel   = cfg.get("val") or cfg.get("valid")
    test_rel  = cfg.get("test")
    nc        = int(cfg.get("nc", 1))
    names     = cfg.get("names")

    assert train_rel and val_rel, "data.yaml must contain train 与 val"
    print("===> data.yaml analyze：")
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

    for p in [train_imgs, train_lbls, val_imgs, val_lbls] + ([test_imgs, test_lbls] if test_imgs else []):
        if p and not p.exists():
            print(f" warning: no such directory：{p}")

    if nc >= 2:
        tr_cnt = quick_class_count(train_lbls, nc)
        va_cnt = quick_class_count(val_lbls, nc)
        print(f"===> Training-set label class counts: {tr_cnt}  (index corresponds to names)")
        print(f"===> Validation-set label class counts: {va_cnt}")
        if sum(tr_cnt[1:]) == 0 and sum(va_cnt[1:]) == 0:
            print(
                " Notice: Detected that classes other than index 0 have almost no bounding boxes. "
                "If 'non-accident' is only a negative sample (no boxes), the more standard approach is to "
                "set nc: 1 and names: ['accident'], and ensure that labels/*.txt for non-accident images are empty."
            )


    model = YOLO("../events/pts/yolov8m.pt")

    workers = pick_workers()
    print("===> workers:", workers)

    train_args = dict(
        data=str(data_yaml),  # YOLO will automatically resolve split paths relative to this file
        epochs=100,
        imgsz=640,  # For 16GB VRAM, 960 is recommended; reduce to 896/640 if memory is tight
        batch=16,  # Let YOLO auto-adjust the effective batch size based on available VRAM
        device=0 if torch.cuda.is_available() else "cpu",
        workers=workers,
        patience=50,
        optimizer="auto",
        lr0=0.005,
        cos_lr=True,
        cache="ram",  # Enable RAM caching (48GB RAM available), speeds up IO
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

    print("===> start training：", {k: v for k, v in train_args.items() if k not in ("optimizer",)})
    results = model.train(**train_args)

    # validate
    print("===> validing ...")
    metrics = model.val(data=str(data_yaml),
                        imgsz=train_args["imgsz"],
                        device=train_args["device"])
    print("===> metrics：", metrics)

    # test
    if test_rel:
        print("===> testing on test set ...")
        model.val(data=str(data_yaml),
                  split="test",
                  imgsz=train_args["imgsz"],
                  device=train_args["device"])

    # output
    save_dir = Path(getattr(results, "save_dir", Path(train_args["project"]) / train_args["name"]))
    best_pt = save_dir / "weights" / "best.pt"
    if best_pt.exists():
        print(f"===> export ONNX（src：{best_pt}） ...")
        YOLO(str(best_pt)).export(format="onnx")
    else:
        print(" unknow")
        model.export(format="onnx")

    print("all success, src is：", save_dir)

if __name__ == "__main__":
    main()
