# # -*- coding: utf-8 -*-
# """
# SessionManager：统一调度所有摄像头任务、检测任务与 SSE 推送
# """
# import asyncio, time, JSON
# from queue import Queue, Empty
# from typing import List, Dict
# from django.http import StreamingHttpResponse, HttpResponse
# import api.runtime_state as rt
# from events.camera_pipeline import SingleFileSession
# from events.Accident_detect.accident_detector import run_accident_detector_multi
#
#
# class SessionManager:
#     GLOBAL_SSE_ID = "sse-main"
#
#     @staticmethod
#     def register(camera_ids: List[str]) -> Dict:
#         """注册相机并准备会话"""
#         results = []
#         for cid in camera_ids:
#             try:
#                 from api.camera_map import get_source
#                 src = get_source(cid)
#                 rt.INTENDED[cid] = src
#                 results.append({"camera_id": cid, "src": src, "ok": True})
#             except Exception as e:
#                 results.append({"camera_id": cid, "ok": False, "error": str(e)})
#
#         rt.SSE_PENDING[SessionManager.GLOBAL_SSE_ID] = list(rt.INTENDED.keys())
#         print(f"[manager] registered cams={camera_ids}")
#         return {
#             "session_id": SessionManager.GLOBAL_SSE_ID,
#             "results": results,
#             "sse_alerts_url": f"/sse/alerts?sse_id={SessionManager.GLOBAL_SSE_ID}",
#             "ts": int(time.time())
#         }
#     @staticmethod
#     def start_all(loop) -> None:
#         """启动所有摄像头任务与检测器"""
#         camera_ids = list(rt.INTENDED.keys())
#         if not camera_ids:
#             print("[manager] no camera registered.")
#             return
#
#         # 启动每路摄像头
#         for cid in camera_ids:
#             if cid not in rt.SESSIONS:
#                 sess = SingleFileSession(
#                     camera_id=cid,
#                     file_path=rt.INTENDED[cid],
#                     bus=rt.BUS,
#                     session_id=SessionManager.GLOBAL_SSE_ID
#                 )
#                 sess.start(loop=loop)
#                 rt.SESSIONS[cid] = sess
#
#         # ✅ 使用当前活跃 session 列表启动检测器
#         active_cams = list(rt.SESSIONS.keys())
#         if not active_cams:
#             print("[manager] no active cameras.")
#             return
#
#         # 启动检测任务（只启动一次）
#         if rt.DETECTOR_TASK is None or rt.DETECTOR_TASK.done():
#             async def _run_multi():
#                 print(f"[manager] detector started for {active_cams}")
#                 await run_accident_detector_multi(
#                     rt.BUS,
#                     camera_ids=active_cams,
#                     batch_size=4,
#                     poll_ms=50,  # 稍微调高，单路时更稳
#                 )
#
#             rt.DETECTOR_TASK = asyncio.run_coroutine_threadsafe(_run_multi(), loop)
#
#     @staticmethod
#     def stop_all(loop) -> List[str]:
#         """停止所有任务"""
#         stopped = []
#         for cid, sess in list(rt.SESSIONS.items()):
#
#             sess.stop(loop=loop)
#             stopped.append(cid)
#             rt.SESSIONS.pop(cid, None)
#             rt.INTENDED.pop(cid, None)
#         if rt.DETECTOR_TASK:
#             rt.DETECTOR_TASK.cancel()
#             rt.DETECTOR_TASK = None
#         print(f"[manager] stopped all cameras")
#         return stopped
#
#     @staticmethod
#     def stream(loop):
#         """创建并返回 StreamingHttpResponse（SSE 推流）"""
#         camera_ids = list(rt.INTENDED.keys())
#         if not camera_ids:
#             return HttpResponse("未注册任何相机源", status=404)
#
#         q: "Queue[bytes]" = Queue(maxsize=256)
#
#         async def _pipe():
#             topics = [f"accidents.open:{cid}" for cid in camera_ids] + \
#                       [f"accidents.close:{cid}" for cid in camera_ids]
#             async with rt.BUS.subscribe_many(topics, mode="fanout", maxsize=64) as subq:
#                 while True:
#                     evt = await subq.get()
#                     cid = evt.get("camera_id", "unknown")
#                     payload = (
#                         f"id: {SessionManager.GLOBAL_SSE_ID}-{int(time.time()*1000)}\n"
#                         f"event: {cid}\n"
#                         f"data: {json.dumps(evt, ensure_ascii=False)}\n\n"
#                     )
#                     try:
#                         q.put_nowait(payload.encode())
#                     except Exception:
#                         try:
#                             _ = q.get_nowait()
#                         except Exception:
#                             pass
#                         q.put_nowait(payload.encode())
#
#         asyncio.run_coroutine_threadsafe(_pipe(), loop)
#
#         def _stream():
#             yield f": connected sse_id={SessionManager.GLOBAL_SSE_ID}\n\n".encode()
#             last = time.time()
#             try:
#                 while True:
#                     try:
#                         chunk = q.get(timeout=1.0)
#                         yield chunk
#                     except Empty:
#                         if time.time() - last >= 10:
#                             yield b": ping\n\n"
#                             last = time.time()
#             finally:
#                 print("[manager] SSE closed")
#
#         resp = StreamingHttpResponse(_stream(), content_type="text/event-stream")
#         resp["Cache-Control"] = "no-cache"
#         resp["X-Accel-Buffering"] = "no"
#         return resp
# -*- coding: utf-8 -*-
# """
# SessionManager：统一调度所有摄像头任务、检测任务与 SSE 推送 + 数据入库
# """
# import asyncio, time, json, random
# from datetime import timedelta
# from queue import Queue, Empty
# from typing import List, Dict
# from django.http import StreamingHttpResponse, HttpResponse
# from asgiref.sync import sync_to_async
# from django.utils import timezone
# import api.runtime_state as rt
#
# from events.models import Event, Camera
# from events.camera_pipeline import SingleFileSession
# from events.Accident_detect.accident_detector import run_accident_detector_multi
#
#
# # ================== 工具函数 ==================
# def _random_recent_ts():
#     """生成最近一周随机时间"""
#     now = timezone.now()
#     delta = timedelta(seconds=random.randint(0, 7 * 24 * 3600))
#     return now - delta
#
#
# def _make_evidence_text(evt: dict) -> str:
#     """根据事件生成自然语言描述"""
#     cam = evt.get("camera_id", "unknown")
#     conf = float(evt.get("peak_confidence", evt.get("confidence", 0.0)))
#     dur = evt.get("duration_sec")
#     if dur is not None:
#         return f"Accident (closed) on {cam}, peak confidence {conf:.2f}, duration {dur:.1f}s."
#     return f"Accident (open) on {cam}, confidence {conf:.2f}."
#
#
# @sync_to_async
# def _save_event_to_db(evt: dict):
#     """异步写入数据库"""
#     cam_id = evt.get("camera_id")
#     if not cam_id:
#         return
#
#     # 确保 camera 存在
#     Camera.objects.get_or_create(camera_id=cam_id)
#
#     ts = _random_recent_ts()
#     conf = float(evt.get("peak_confidence", evt.get("confidence", 0.0)))
#     text = _make_evidence_text(evt)
#
#     Event.objects.create(
#         timestamp=ts,
#         camera_id=cam_id,
#         type="accident",
#         weather="clear",         # 暂无天气模块
#         confidence=conf,
#         evidence_text=text,
#         embedding=None,       # RAG 预留
#     )
#     print(f"[DB] saved accident event for {cam_id} ({conf:.2f})")
#
#
# # ================== 核心管理类 ==================
# class SessionManager:
#     GLOBAL_SSE_ID = "sse-main"
#
#     # ---------- 注册 ----------
#     @staticmethod
#     def register(camera_ids: List[str]) -> Dict:
#         """注册相机并准备会话"""
#         results = []
#         for cid in camera_ids:
#             try:
#                 from api.camera_map import get_source
#                 src = get_source(cid)
#                 rt.INTENDED[cid] = src
#                 results.append({"camera_id": cid, "src": src, "ok": True})
#             except Exception as e:
#                 results.append({"camera_id": cid, "ok": False, "error": str(e)})
#
#         rt.SSE_PENDING[SessionManager.GLOBAL_SSE_ID] = list(rt.INTENDED.keys())
#         print(f"[manager] registered cams={camera_ids}")
#         return {
#             "session_id": SessionManager.GLOBAL_SSE_ID,
#             "results": results,
#             "sse_alerts_url": f"/sse/alerts?sse_id={SessionManager.GLOBAL_SSE_ID}",
#             "ts": int(time.time()),
#         }
#
#     # ---------- 启动 ----------
#     @staticmethod
#     def start_all(loop) -> None:
#         """启动所有摄像头任务与检测器"""
#         camera_ids = list(rt.INTENDED.keys())
#         if not camera_ids:
#             print("[manager] no camera registered.")
#             return
#
#         # 启动每路摄像头
#         for cid in camera_ids:
#             if cid not in rt.SESSIONS:
#                 sess = SingleFileSession(
#                     camera_id=cid,
#                     file_path=rt.INTENDED[cid],
#                     bus=rt.BUS,
#                     session_id=SessionManager.GLOBAL_SSE_ID,
#                 )
#                 sess.start(loop=loop)
#                 rt.SESSIONS[cid] = sess
#
#         # 启动检测器
#         active_cams = list(rt.SESSIONS.keys())
#         if not active_cams:
#             print("[manager] no active cameras.")
#             return
#
#         if rt.DETECTOR_TASK is None or rt.DETECTOR_TASK.done():
#             async def _run_multi():
#                 print(f"[manager] detector started for {active_cams}")
#                 await run_accident_detector_multi(
#                     rt.BUS,
#                     camera_ids=active_cams,
#                     batch_size=4,
#                     poll_ms=50,
#                 )
#
#             rt.DETECTOR_TASK = asyncio.run_coroutine_threadsafe(_run_multi(), loop)
#
#     # ---------- 停止 ----------
#     @staticmethod
#     def stop_all(loop) -> List[str]:
#         """停止所有任务"""
#         stopped = []
#         for cid, sess in list(rt.SESSIONS.items()):
#             sess.stop(loop=loop)
#             stopped.append(cid)
#             rt.SESSIONS.pop(cid, None)
#             rt.INTENDED.pop(cid, None)
#         if rt.DETECTOR_TASK:
#             rt.DETECTOR_TASK.cancel()
#             rt.DETECTOR_TASK = None
#         print(f"[manager] stopped all cameras")
#         return stopped
#
#     # ---------- SSE 流 ----------
#     @staticmethod
#     def stream(loop):
#         """创建并返回 StreamingHttpResponse（SSE 推流 + 入库）"""
#         camera_ids = list(rt.INTENDED.keys())
#         if not camera_ids:
#             return HttpResponse("未注册任何相机源", status=404)
#
#         q: "Queue[bytes]" = Queue(maxsize=256)
#
#         async def _pipe():
#             topics = [f"accidents.open:{cid}" for cid in camera_ids] + \
#                       [f"accidents.close:{cid}" for cid in camera_ids]
#             async with rt.BUS.subscribe_many(topics, mode="fanout", maxsize=64) as subq:
#                 while True:
#                     evt = await subq.get()
#
#                     # ✅ 入库逻辑（只保存开案事件）
#                     if evt.get("type") == "accident_open":
#                         try:
#                             await _save_event_to_db(evt)
#                         except Exception as e:
#                             print(f"[DB] save failed: {e}")
#
#                     # 🔄 保持原有 SSE 推送
#                     cid = evt.get("camera_id", "unknown")
#                     payload = (
#                         f"id: {SessionManager.GLOBAL_SSE_ID}-{int(time.time()*1000)}\n"
#                         f"event: {cid}\n"
#                         f"data: {json.dumps(evt, ensure_ascii=False)}\n\n"
#                     )
#                     try:
#                         q.put_nowait(payload.encode())
#                     except Exception:
#                         try:
#                             _ = q.get_nowait()
#                         except Exception:
#                             pass
#                         q.put_nowait(payload.encode())
#
#         asyncio.run_coroutine_threadsafe(_pipe(), loop)
#
#         def _stream():
#             yield f": connected sse_id={SessionManager.GLOBAL_SSE_ID}\n\n".encode()
#             last = time.time()
#             try:
#                 while True:
#                     try:
#                         chunk = q.get(timeout=1.0)
#                         yield chunk
#                     except Empty:
#                         if time.time() - last >= 10:
#                             yield b": ping\n\n"
#                             last = time.time()
#             finally:
#                 print("[manager] SSE closed")
#
#         resp = StreamingHttpResponse(_stream(), content_type="text/event-stream")
#         resp["Cache-Control"] = "no-cache"
#         resp["X-Accel-Buffering"] = "no"
#         return resp
# -*- coding: utf-8 -*-
"""
SessionManager：统一调度摄像头任务、检测器与 SSE 推送
+ 后台入库（随机近一周时间、weather=clear、evidence_text）
+ 实时生成 embedding（bge-base-en-v1.5 / 768 维）
"""
from __future__ import annotations

import asyncio, time, json, random
from queue import Queue, Empty
from typing import List, Dict
from datetime import timedelta

from django.http import StreamingHttpResponse, HttpResponse
from django.utils import timezone
from asgiref.sync import sync_to_async

import api.runtime_state as rt
from events.camera_pipeline import SingleFileSession
from events.Accident_detect.accident_detector import run_accident_detector_multi
from events.models import Event, Camera

# ---------- 嵌入模型（768 维） ----------
from sentence_transformers import SentenceTransformer
import numpy as np

_EMBED_MODEL: SentenceTransformer | None = None

def _get_embedder() -> SentenceTransformer:
    global _EMBED_MODEL
    if _EMBED_MODEL is None:
        # 首次会下载权重；常驻内存
        _EMBED_MODEL = SentenceTransformer("BAAI/bge-base-en-v1.5")
    return _EMBED_MODEL

def _embed_text(text: str) -> list[float]:
    emb = _get_embedder().encode(text, normalize_embeddings=True)  # np.ndarray (768,)
    return emb.astype(np.float32).tolist()

# ---------- 工具 ----------
def _random_recent_ts():
    now = timezone.now()
    return now - timedelta(seconds=random.randint(0, 7 * 24 * 3600))

def _make_evidence_text(evt: dict) -> str:
    cam = evt.get("camera_id", "unknown")
    conf = float(evt.get("peak_confidence", evt.get("confidence", 0.0)))
    dur = evt.get("duration_sec")
    return (f"Accident (closed) on {cam}, peak confidence {conf:.2f}, duration {float(dur):.1f}s."
            if dur is not None else f"Accident (open) on {cam}, confidence {conf:.2f}.")

# 后台入库队列
SAVE_QUEUE: asyncio.Queue = asyncio.Queue(maxsize=512)

@sync_to_async
def _save_event_to_db(evt: dict):
    """执行一次事件入库：保证 camera、生成 evidence_text 与 embedding"""
    cam_id = evt.get("camera_id")
    if not cam_id:
        return

    # 确保 camera 存在（映射到 db_table='cameras'）
    Camera.objects.get_or_create(camera_id=cam_id)

    ts = _random_recent_ts()
    conf = float(evt.get("peak_confidence", evt.get("confidence", 0.0)))
    text = _make_evidence_text(evt)

    # 生成 768 维向量（与 VECTOR(768) 对齐）
    try:
        vec = _embed_text(text)
    except Exception as e:
        print(f"[EMB] embed failed: {e}")
        vec = None

    # 写入 events 表（db_table='events'）
    Event.objects.create(
        timestamp=ts,
        camera_id=cam_id,          # 外键列名 camera_id（模型里 db_column 已指定）
        type="accident",
        weather="clear",           # 暂无天气检测：默认 clear
        confidence=conf,
        evidence_text=text,
        embedding=vec,             # 实时写入 embedding
    )
    print(f"[DB] saved accident event for {cam_id} ({conf:.2f})")

async def _save_worker():
    """后台消费者：从 SAVE_QUEUE 取事件并入库，不阻塞 SSE。"""
    while True:
        evt = await SAVE_QUEUE.get()
        try:
            await _save_event_to_db(evt)
        except Exception as e:
            print(f"[save_worker] DB error: {e}")
        finally:
            SAVE_QUEUE.task_done()

# ================== 核心管理类 ==================
class SessionManager:
    GLOBAL_SSE_ID = "sse-main"

    @staticmethod
    def register(camera_ids: List[str]) -> Dict:
        results = []
        for cid in camera_ids:
            try:
                from api.camera_map import get_source
                src = get_source(cid)
                rt.INTENDED[cid] = src
                results.append({"camera_id": cid, "src": src, "ok": True})
            except Exception as e:
                results.append({"camera_id": cid, "ok": False, "error": str(e)})

        rt.SSE_PENDING[SessionManager.GLOBAL_SSE_ID] = list(rt.INTENDED.keys())
        print(f"[manager] registered cams={camera_ids}")
        return {
            "session_id": SessionManager.GLOBAL_SSE_ID,
            "results": results,
            "sse_alerts_url": f"/sse/alerts?sse_id={SessionManager.GLOBAL_SSE_ID}",
            "ts": int(time.time()),
        }

    @staticmethod
    def start_all(loop) -> None:
        camera_ids = list(rt.INTENDED.keys())
        if not camera_ids:
            print("[manager] no camera registered.")
            return

        for cid in camera_ids:
            if cid not in rt.SESSIONS:
                sess = SingleFileSession(
                    camera_id=cid,
                    file_path=rt.INTENDED[cid],
                    bus=rt.BUS,
                    session_id=SessionManager.GLOBAL_SSE_ID,
                )
                sess.start(loop=loop)
                rt.SESSIONS[cid] = sess

        active_cams = list(rt.SESSIONS.keys())
        if not active_cams:
            print("[manager] no active cameras.")
            return

        if rt.DETECTOR_TASK is None or rt.DETECTOR_TASK.done():
            async def _run_multi():
                print(f"[manager] detector started for {active_cams}")
                await run_accident_detector_multi(
                    rt.BUS,
                    camera_ids=active_cams,
                    batch_size=4,
                    poll_ms=50,
                )
            rt.DETECTOR_TASK = asyncio.run_coroutine_threadsafe(_run_multi(), loop)

    @staticmethod
    def stop_all(loop) -> List[str]:
        stopped = []
        for cid, sess in list(rt.SESSIONS.items()):
            sess.stop(loop=loop)
            stopped.append(cid)
            rt.SESSIONS.pop(cid, None)
            rt.INTENDED.pop(cid, None)
        if rt.DETECTOR_TASK:
            rt.DETECTOR_TASK.cancel()
            rt.DETECTOR_TASK = None
        print(f"[manager] stopped all cameras")
        return stopped

    @staticmethod
    def stream(loop):
        """SSE 推送 + 入库（后台队列）"""
        camera_ids = list(rt.INTENDED.keys())
        if not camera_ids:
            return HttpResponse("未注册任何相机源", status=404)

        # 启动后台入库 worker（一次）
        if not hasattr(rt, "SAVE_WORKER") or rt.SAVE_WORKER is None or rt.SAVE_WORKER.done():
            rt.SAVE_WORKER = asyncio.run_coroutine_threadsafe(_save_worker(), loop)

        q: "Queue[bytes]" = Queue(maxsize=256)

        async def _pipe():
            topics = [f"accidents.open:{cid}" for cid in camera_ids] + \
                     [f"accidents.close:{cid}" for cid in camera_ids]
            async with rt.BUS.subscribe_many(topics, mode="fanout", maxsize=64) as subq:
                while True:
                    evt = await subq.get()

                    # 入库：仅 open（close 可按需扩展）
                    if evt.get("type") == "accident_open":
                        try:
                            SAVE_QUEUE.put_nowait(evt)   # 非阻塞；队满时可按需丢弃策略
                        except asyncio.QueueFull:
                            print("[warn] SAVE_QUEUE full, dropping event")

                    # 原有 SSE 推送
                    cid = evt.get("camera_id", "unknown")
                    payload = (
                        f"id: {SessionManager.GLOBAL_SSE_ID}-{int(time.time()*1000)}\n"
                        f"event: {cid}\n"
                        f"data: {json.dumps(evt, ensure_ascii=False)}\n\n"
                    )
                    try:
                        q.put_nowait(payload.encode())
                    except Exception:
                        try:
                            _ = q.get_nowait()
                        except Exception:
                            pass
                        q.put_nowait(payload.encode())

        asyncio.run_coroutine_threadsafe(_pipe(), loop)

        def _stream():
            yield f": connected sse_id={SessionManager.GLOBAL_SSE_ID}\n\n".encode()
            last = time.time()
            try:
                while True:
                    try:
                        chunk = q.get(timeout=1.0)
                        yield chunk
                    except Empty:
                        if time.time() - last >= 10:
                            yield b": ping\n\n"
                            last = time.time()
            finally:
                print("[manager] SSE closed")

        resp = StreamingHttpResponse(_stream(), content_type="text/event-stream")
        resp["Cache-Control"] = "no-cache"
        resp["X-Accel-Buffering"] = "no"
        return resp
