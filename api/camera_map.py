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

CAMERA_SOURCES: dict[str, dict] = {
    "cam-1": {
        "src": r"E:\Training\Recording 2025-11-16 141655.mp4",
    },
    "cam-2": {
        "src": r"E:\Training\Recording 2025-11-02 172630.mp4",
    },
    "cam-3": {
        "src": r"E:\Training\Recording 2025-11-14 231727.mp4",
    },
    "cam-4": {
        "src": r"E:\Training\Recording 2025-11-16 140258.mp4",
    },
    "cam-5": {
        "src": r"E:\Training\cam-5.mp4",
    }
}

def get_source(camera_id: str) -> str:
    """Lookup the table and validate the entry; raise an exception if the camera is not configured or disabled."""
    meta = CAMERA_SOURCES.get(camera_id)
    if not meta or not meta.get("enabled", True):
        raise KeyError(f"camera_id is not configured or is disabled: {camera_id}")
    src = (meta.get("src") or "").strip()
    if not src:
        raise ValueError(f"camera_id does not provide a valid src: {camera_id}")
    return src
