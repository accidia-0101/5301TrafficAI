# web_player.py — 提供浏览器播放页 & 静态服务 HLS 分片
from __future__ import annotations
import asyncio, os, mimetypes
from pathlib import Path
from aiohttp import web

# HLS 根目录（映射到 /hls/）
DEFAULT_HLS_ROOT = Path(r"E:\Training\hls_out").resolve()

PLAYER_HTML = """<!doctype html>
<html lang="zh">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>TrafficAI HLS 播放器</title>
  <style>
    body{background:#111;color:#eee;font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial}
    .wrap{max-width:960px;margin:24px auto;padding:16px}
    video{width:100%;background:#000;border-radius:12px}
    .row{display:flex;gap:8px;align-items:center;margin:12px 0}
    input,button,select{padding:8px 10px;border-radius:8px;border:1px solid #444;background:#222;color:#eee}
    button{cursor:pointer}
    small{color:#9aa}
  </style>
</head>
<body>
<div class="wrap">
  <h2>TrafficAI HLS 播放器</h2>
  <div class="row">
    <label>播放地址：</label>
    <input id="src" style="flex:1" value="/hls/cam-1/index.m3u8"/>
    <button id="play">播放</button>
    <label style="margin-left:8px">低延迟</label>
    <input id="ll" type="checkbox" checked />
  </div>
  <video id="v" controls autoplay playsinline muted></video>
  <div class="row"><small>把你的 HLS 输出目录映射为 /hls/，例如 E:\\Training\\hls_out\\cam-1\\index.m3u8 → /hls/cam-1/index.m3u8</small></div>
</div>
<script src="https://cdn.jsdelivr.net/npm/hls.js@latest"></script>
<script>
const $ = s => document.querySelector(s);
const v = $("#v"); const src = $("#src"); const btn = $("#play"); const ll = $("#ll");
function play(url){
  if (v.canPlayType("application/vnd.apple.mpegurl")) {
    v.src = url;
  } else if (Hls.isSupported()) {
    if (window._hls) { window._hls.destroy(); }
    const cfg = ll.checked ? { lowLatencyMode:true, liveSyncDuration:1 } : {};
    const hls = new Hls(cfg);
    window._hls = hls;
    hls.loadSource(url);
    hls.attachMedia(v);
    hls.on(Hls.Events.ERROR, (e, data) => console.error("HLS.js error:", data));
  } else {
    alert("此浏览器不支持 HLS");
  }
}
btn.onclick = () => play(src.value);
const qp = new URLSearchParams(location.search);
if (qp.get("url")) { src.value = qp.get("url"); }
play(src.value);
</script>
</body></html>
"""

async def create_app(hls_root: Path = DEFAULT_HLS_ROOT) -> web.Application:
    app = web.Application()

    # 修正常见扩展的 MIME（某些系统可能缺少）
    mimetypes.add_type("application/vnd.apple.mpegurl", ".m3u8")
    mimetypes.add_type("video/MP2T", ".ts")
    mimetypes.add_type("video/mp4", ".mp4")
    mimetypes.add_type("application/octet-stream", ".m4s")

    async def index(_req):  # 播放器页面
        return web.Response(text=PLAYER_HTML, content_type="text/html",charset="utf-8")

    app.router.add_get("/", index)

    # 静态映射：/hls/* → 本地 hls_root/*
    if not hls_root.exists():
        hls_root.mkdir(parents=True, exist_ok=True)
    app.router.add_static("/hls/", path=str(hls_root), show_index=True)

    # 允许跨域（如需）
    @web.middleware
    async def cors_mw(request, handler):
        resp = await handler(request)
        resp.headers["Access-Control-Allow-Origin"] = "*"
        resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        return resp
    app.middlewares.append(cors_mw)
    return app

async def run_server(host="127.0.0.1", port=8000, hls_root: str | os.PathLike = DEFAULT_HLS_ROOT):
    app = await create_app(Path(hls_root))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    print(f"🌐 Web: http://{host}:{port}  （播放器）")
    print(f"📁 HLS: http://{host}:{port}/hls/  映射到 {Path(hls_root).resolve()}")
    # 常驻
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(run_server())
