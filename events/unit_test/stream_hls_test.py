# hls_test.py (hardened)
import asyncio, os, sys, contextlib, logging


# 你的项目依赖
from events.bus import AsyncBus
from events.frame_discrete import run_frame_source_raw   # 发布到 'frames_raw'
from events.stream_hls import HlsStreamer                # 推 HLS 的类（内部用 ffmpeg）

VIDEO_PATH = r"E:\Training\Recording 2025-11-02 152123.mp4"
CAMERA_ID = "cam-1"
HLS_ROOT = r"E:\Training\hls_out"
TARGET_FPS = 30
USE_NVENC = True

# ---- 统一任务跟踪：防止“Future exception was never retrieved” ----
_tasks = set()

def track_task(coro, *, name=None):
    t = asyncio.create_task(coro, name=name)
    _tasks.add(t)
    def _done(task: asyncio.Task):
        _tasks.discard(task)
        try:
            exc = task.exception()
            if exc:
                logging.exception("Task %s crashed:", task.get_name() or "<unnamed>", exc_info=exc)
        except asyncio.CancelledError:
            pass
    t.add_done_callback(_done)
    return t

def observe_task(t: asyncio.Task, *, label="task"):
    """给已存在的 Task 挂一个异常观测器。"""
    def _done(task: asyncio.Task):
        try:
            exc = task.exception()
            if exc:
                logging.exception("%s crashed:", label, exc_info=exc)
        except asyncio.CancelledError:
            pass
    t.add_done_callback(_done)
    return t

def setup_loop_exception_logger(loop: asyncio.AbstractEventLoop):
    def handle_loop_exc(loop, context):
        msg = context.get("message", "")
        exc = context.get("exception")
        logging.error("Loop exception: %s", msg, exc_info=exc)
    loop.set_exception_handler(handle_loop_exc)

# ---- aiohttp 静态服：封装成 async 上下文，确保收尾 ----
# class StaticServer:
#     def __init__(self, root: str, mount="/hls", host="127.0.0.1", port=8080):
#         self.root, self.mount, self.host, self.port = root, mount, host, port
#         self._runner = None
#         self._site = None
#
#     async def __aenter__(self):
#         try:
#             from aiohttp import web
#         except Exception as e:
#             print(f"⚠️ 静态服务器未启用（仅影响浏览器访问）：{e}")
#             return self
#         app = web.Application()
#         app.router.add_static(self.mount, path=self.root, show_index=True)
#         self._runner = web.AppRunner(app)
#         await self._runner.setup()
#         self._site = web.TCPSite(self._runner, host=self.host, port=self.port)
#         await self._site.start()
#         print(f"🌐 打开: http://{self.host}:{self.port}{self.mount}/{CAMERA_ID}/index.m3u8")
#         return self
#
#     async def __aexit__(self, exc_type, exc, tb):
#         with contextlib.suppress(Exception):
#             if self._runner:
#                 await self._runner.cleanup()
#         self._runner = None
#         self._site = None
class StaticServer:
    def __init__(self, root: str, mount="/hls", host="127.0.0.1", port=8080, camera_id="cam-1"):
        self.root, self.mount, self.host, self.port = root, mount, host, port
        self.camera_id = camera_id
        self._runner = None
        self._site = None

    async def __aenter__(self):
        from aiohttp import web

        # 简单 CORS（即便将来跨域访问也不报错）
        @web.middleware
        async def cors_mw(request, handler):
            resp = await handler(request)
            resp.headers["Access-Control-Allow-Origin"] = "*"
            resp.headers["Access-Control-Allow-Headers"] = "*"
            resp.headers["Access-Control-Allow-Methods"] = "GET,OPTIONS"
            return resp

        app = web.Application(middlewares=[cors_mw])

        # 静态目录：/hls → HLS 片段
        app.router.add_static(self.mount, path=self.root, show_index=True)

        # 同源播放器页：/player/{cam}
        async def player(req: web.Request):
            cam = req.match_info.get("cam", self.camera_id)
            m3u8 = f"{self.mount}/{cam}/index.m3u8"
            html = f"""<!doctype html><meta charset="utf-8">
            <title>HLS Player - {cam}</title>
            <body style="background:#111;color:#eee;font-family:sans-serif">
            <h2 style="text-align:center">HLS 播放：{cam}</h2>
            <video id="v" controls autoplay muted playsinline
                   style="display:block;margin:8px auto;width:80vw;background:#000"></video>
            <script src="https://cdn.jsdelivr.net/npm/hls.js@latest"></script>
            <script>
            const url = "{m3u8}";
            const v = document.getElementById("v");

            // Safari 原生 HLS
            if (v.canPlayType("application/vnd.apple.mpegurl")) {{
              v.src = url;
              v.addEventListener("loadedmetadata", () => {{
                try {{ v.currentTime = 0; }} catch (e) {{}}
              }});
            }}
            // 其他浏览器用 hls.js
            else if (Hls.isSupported()) {{
              const hls = new Hls({{
                lowLatencyMode: false,   // Event/DVR 模式建议关闭 LLL
                startPosition: 0         // 起播从 0
              }});
              hls.loadSource(url);
              hls.attachMedia(v);
              hls.on(Hls.Events.MANIFEST_PARSED, () => {{
                try {{ v.currentTime = 0; }} catch (e) {{}}
              }});
              hls.on(Hls.Events.ERROR, (e, data) => console.error("HLS.js error:", data));
            }} else {{
              document.body.insertAdjacentHTML(
                "beforeend",
                '<p style="text-align:center">此浏览器不支持 HLS。</p>'
              );
            }}
            </script>
            </body>"""

            return web.Response(text=html, content_type="text/html")

        app.router.add_get("/player/{cam}", player)
        app.router.add_get("/player", player)  # 默认用 camera_id

        self._runner = web.AppRunner(app)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, host=self.host, port=self.port)
        await self._site.start()

        print(f"🌐 打开播放器: http://{self.host}:{self.port}/player/{self.camera_id}")
        print(f"🌐 或直接 m3u8: http://{self.host}:{self.port}{self.mount}/{self.camera_id}/index.m3u8")
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self._runner:
            await self._runner.cleanup()
        self._runner = self._site = None

async def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    print("🚀 启动 HLS 推流测试")
    loop = asyncio.get_event_loop()
    setup_loop_exception_logger(loop)

    bus = AsyncBus()

    # 1) 帧源（全帧）→ 'frames_raw'
    #    注意：用 track_task 跟踪，异常自动记录
    t_source = track_task(
        run_frame_source_raw(bus, CAMERA_ID, VIDEO_PATH),
        name="frame_source_raw"
    )

    # 2) HLS 推流（订阅 'frames_raw'）
    streamer = HlsStreamer(
        bus=bus,
        camera_id=CAMERA_ID,
        out_dir=HLS_ROOT,
        fps=TARGET_FPS,
        width=None,     # 不指定则保持源尺寸；固定输出时请保证偶数（yuv420p）
        height=None,
        use_nvenc=USE_NVENC,
        gop_seconds=2,  # I 帧间隔
        hls_time=1.0,   # 每片 1 秒
        keep_last=6     # 播放列表保留最近 6 片
    )
    streamer.start()

    # 让已有的内部任务也被观测，避免“Future exception was never retrieved”
    if getattr(streamer, "_task", None) is not None:
        observe_task(streamer._task, label="HlsStreamer._task")

    # 3) 静态文件服务器（可选）
    async with StaticServer(HLS_ROOT):
        # 4) 等待任务；用 return_exceptions=True，防止未捕获异常导致崩溃
        try:
            await asyncio.gather(t_source, streamer._task, return_exceptions=True)
        except asyncio.CancelledError:
            pass
        finally:
            # 优雅关停：先停推流，再停帧源，最后收尸所有任务
            with contextlib.suppress(Exception):
                streamer.cancel()   # 若有 async close() 可改为 await streamer.close()
            for t in list(_tasks):
                t.cancel()
            await asyncio.gather(*_tasks, return_exceptions=True)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 手动中止")
