# rag/logic/rag_service.py
from sentence_transformers import SentenceTransformer
from pgvector.django import CosineDistance
from events.models import Event,Camera

# 单例缓存模型
_embedder = None


def get_embedder():
    global _embedder
    if _embedder is None:
        _embedder = SentenceTransformer("BAAI/bge-base-en-v1.5")
    return _embedder


def search_similar_events(query: str, top_k: int = 5):

    model = get_embedder()
    q_vec = model.encode(query, normalize_embeddings=True).tolist()

    # 1) 自动识别 camera id（简单规则）
    cam_id = None
    for cam in Camera.objects.values_list("camera_id", flat=True):
        if cam.lower() in query.lower():
            cam_id = cam
            break

    qs = Event.objects.all()

    # 2) 如果识别到 camera，加过滤（避免无关事件）
    if cam_id:
        qs = qs.filter(camera__camera_id=cam_id)

    # 3) 加相似度距离（避免完全不相关的事件）
    d = CosineDistance("embedding", q_vec)
    qs = qs.annotate(dist=d).filter(dist__lt=0.6).order_by("dist")[:top_k]

    # 4) 返回格式
    results = []
    for e in qs:
        results.append({
            "event_id": str(e.event_id),
            "timestamp": e.timestamp.isoformat(),
            "camera": e.camera.camera_id,
            "type": e.type,
            "weather": e.weather or "clear",
            "confidence": float(e.confidence),
            "text": e.evidence_text,
            "similarity": float(1 - e.dist),   # 可选：相似度
        })

    return results

