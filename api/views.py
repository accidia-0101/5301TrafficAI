# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import time

from django.http import HttpRequest
from django.views.decorators.http import require_GET
# api/views_rag.py
from django.views.decorators.http import require_POST

import api.runtime_state as rt
from api.session_manager import SessionManager
from rag.local_llm import generate_local_answer
from rag.rag_service import search_similar_events


def _json(req: HttpRequest) -> dict:
    try:
        return json.loads(req.body.decode("utf-8")) if req.body else {}
    except Exception:
        return {}

from django.views.decorators.csrf import csrf_exempt
from django.http import JsonResponse


# -------- /api/play --------
@csrf_exempt
@require_POST
def play_view(request: HttpRequest):
    body = _json(request)
    ids = []
    if isinstance(body.get("camera_id"), str):
        ids.append(body["camera_id"].strip())
    if isinstance(body.get("camera_ids"), list):
        ids += [x.strip() for x in body["camera_ids"] if isinstance(x, str) and x.strip()]
    if not ids:
        return JsonResponse({"ok": False, "error": "camera_id 缺失"}, status=400)

    data = SessionManager.register(ids)
    return JsonResponse({"ok": True, **data})


# -------- /sse/alerts --------
@csrf_exempt
@require_GET
def alerts_stream(request: HttpRequest):
    loop = rt.ensure_bg_loop()
    SessionManager.start_all(loop)
    return SessionManager.stream(loop)


# -------- /api/stop --------
@csrf_exempt
@require_POST
def stop_view(request: HttpRequest):
    loop = rt.ensure_bg_loop()
    stopped = SessionManager.stop_all(loop)
    return JsonResponse({
        "ok": True,
        "sse_id": SessionManager.GLOBAL_SSE_ID,
        "stopped_cameras": stopped,
        "ts": int(time.time())
    })


@csrf_exempt
@require_POST
def ask_view(request):
    body = json.loads(request.body or "{}")
    query = body.get("query", "").strip()
    if not query:
        return JsonResponse({"error": "empty query"}, status=400)

    # 检索事件
    results = search_similar_events(query)

    # 拼接 evidence_text
    context = "\n".join([f"[{r['camera']} {r['timestamp']}] {r['text']}" for r in results])

    # 生成回答
    answer = generate_local_answer(query, context)

    return JsonResponse({
        "query": query,
        "answer": answer,
        "results": results,
    })

