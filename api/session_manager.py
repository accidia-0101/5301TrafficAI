# -*- coding: utf-8 -*-
"""
SessionManager：统一调度所有摄像头任务、检测任务与 SSE 推送
"""
import asyncio, time, json
from queue import Queue, Empty
from typing import List, Dict
from django.http import StreamingHttpResponse, HttpResponse
import api.runtime_state as rt
from events.camera_pipeline import SingleFileSession
from events.Accident_detect.accident_detector import run_accident_detector_multi


class SessionManager:
    GLOBAL_SSE_ID = "sse-main"

    @staticmethod
    def register(camera_ids: List[str]) -> Dict:
        """注册相机并准备会话"""
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
            "ts": int(time.time())
        }

    # @staticmethod
    # def start_all(loop) -> None:
    #     """启动所有摄像头任务与检测器"""
    #     camera_ids = list(rt.INTENDED.keys())
    #     if not camera_ids:
    #         print("[manager] no camera registered.")
    #         return
    #
    #     # 启动每路摄像头
    #     for cid in camera_ids:
    #         if cid not in rt.SESSIONS:
    #             sess = SingleFileSession(camera_id=cid, file_path=rt.INTENDED[cid],
    #                                      bus=rt.BUS, session_id=SessionManager.GLOBAL_SSE_ID)
    #             sess.start(loop=loop)
    #             rt.SESSIONS[cid] = sess
    #
    #     # 启动检测任务（只启动一次）
    #     if rt.DETECTOR_TASK is None or rt.DETECTOR_TASK.done():
    #         async def _run_multi():
    #             await run_accident_detector_multi(rt.BUS, camera_ids=camera_ids,
    #                                               batch_size=4, poll_ms=20)
    #         rt.DETECTOR_TASK = asyncio.run_coroutine_threadsafe(_run_multi(), loop)
    #         print(f"[manager] detector started for {camera_ids}")
    @staticmethod
    def start_all(loop) -> None:
        """启动所有摄像头任务与检测器"""
        camera_ids = list(rt.INTENDED.keys())
        if not camera_ids:
            print("[manager] no camera registered.")
            return

        # 启动每路摄像头
        for cid in camera_ids:
            if cid not in rt.SESSIONS:
                sess = SingleFileSession(
                    camera_id=cid,
                    file_path=rt.INTENDED[cid],
                    bus=rt.BUS,
                    session_id=SessionManager.GLOBAL_SSE_ID
                )
                sess.start(loop=loop)
                rt.SESSIONS[cid] = sess

        # ✅ 使用当前活跃 session 列表启动检测器
        active_cams = list(rt.SESSIONS.keys())
        if not active_cams:
            print("[manager] no active cameras.")
            return

        # 启动检测任务（只启动一次）
        if rt.DETECTOR_TASK is None or rt.DETECTOR_TASK.done():
            async def _run_multi():
                print(f"[manager] detector started for {active_cams}")
                await run_accident_detector_multi(
                    rt.BUS,
                    camera_ids=active_cams,
                    batch_size=4,
                    poll_ms=50,  # 稍微调高，单路时更稳
                )

            rt.DETECTOR_TASK = asyncio.run_coroutine_threadsafe(_run_multi(), loop)

    @staticmethod
    def stop_all(loop) -> List[str]:
        """停止所有任务"""
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
        """创建并返回 StreamingHttpResponse（SSE 推流）"""
        camera_ids = list(rt.INTENDED.keys())
        if not camera_ids:
            return HttpResponse("未注册任何相机源", status=404)

        q: "Queue[bytes]" = Queue(maxsize=256)

        async def _pipe():
            topics = [f"accidents.open:{cid}" for cid in camera_ids] + \
                      [f"accidents.close:{cid}" for cid in camera_ids]
            async with rt.BUS.subscribe_many(topics, mode="fanout", maxsize=64) as subq:
                while True:
                    evt = await subq.get()
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
