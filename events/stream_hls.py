# stream_hls.py
"""
订阅 bus 的 'frames_raw'（原始解码帧，不降帧、不采样）→ 通过 FFmpeg stdin 推 HLS 到目录。
- 设计要点：
  1) 订阅 'frames_raw'，mode='fifo'，避免启动瞬间丢掉开头帧，保证“从头开始”的一致性；
  2) 输出目录在会话开始时可选清空（防止残留分片让前端从中间开始）；
  3) 以 pts_in_video=0 对齐时间轴；检测侧使用 'frames'（采样）得到的事件用同一 pts 对齐；
  4) 优先 NVENC（5070Ti 可用），兜底 libx264。
"""

from __future__ import annotations
import asyncio, os, shutil, subprocess, signal
from dataclasses import dataclass
from typing import Optional
import numpy as np

import cv2
from events.bus import AsyncBus, Frame

def _ffmpeg_bin() -> str:
    return "ffmpeg.exe" if os.name == "nt" else "ffmpeg"

def _nvenc_available_default() -> bool:
    # 你的机子是 5070Ti，这里默认 True；如需保守，可做实际探测。
    return True

@dataclass
class HLSTarget:
    out_dir: str                     # e.g. r"E:\Training\hls_out\cam-1"
    m3u8_name: str = "index.m3u8"
    seg_time: float = 1.0            # 分片秒数（1s 更低延迟）
    list_size: int = 6               # 清单长度
    cleanup_on_start: bool = True    # 会话开始先清空目录，确保“从头开始”无残留
    use_fmp4: bool = False           # 初期用 .ts 更稳；如需 CMAF 可改 True

class HLSPusher:
    """
    'frames_raw' → HLS 编码器：从会话开头帧开始编码，不丢开头，时间轴用 pts_in_video。
    """
    def __init__(
        self,
        bus: AsyncBus,
        camera_id: str,
        *,
        target: HLSTarget,
        enc_fps: Optional[float] = None,     # 如传 None，将自动以第一段 pts 差估计；不严苛
        force_size: Optional[tuple[int,int]] = None,  # 统一编码尺寸 (w,h)。None=用源帧尺寸
        prefer_nvenc: Optional[bool] = None,
        gop_seconds: float = 2.0,
        qp: int = 23,            # NVENC: constqp QP
        x264_crf: int = 20,      # x264: CRF
        profile: str = "high",
        preset_nvenc: str = "p4",
        tune_nvenc: str = "hq",
        preset_x264: str = "veryfast",
        tune_x264: str = "zerolatency",
    ):
        self.bus = bus
        self.camera_id = camera_id
        self.target = target
        self.enc_fps = enc_fps
        self.force_size = force_size
        self.prefer_nvenc = _nvenc_available_default() if prefer_nvenc is None else prefer_nvenc
        self.gop_seconds = max(0.5, gop_seconds)
        self.qp = qp
        self.x264_crf = x264_crf
        self.profile = profile
        self.preset_nvenc = preset_nvenc
        self.tune_nvenc = tune_nvenc
        self.preset_x264 = preset_x264
        self.tune_x264 = tune_x264

        self._proc: Optional[subprocess.Popen] = None
        self._stop = asyncio.Event()
        self._auto_fps_ready = False
        self._last_pts: Optional[float] = None
        self._auto_fps_samples: list[float] = []

    # ---------- FFmpeg 子进程 ----------
    def _build_cmd(self, w: int, h: int, fps: float) -> list[str]:
        exe = _ffmpeg_bin()
        out_dir = os.path.abspath(self.target.out_dir)
        os.makedirs(out_dir, exist_ok=True)
        out_m3u8 = os.path.join(out_dir, self.target.m3u8_name)
        seg_pat = os.path.join(out_dir, "seg_%05d" + (".m4s" if self.target.use_fmp4 else ".ts"))

        # hls_flags = ["delete_segments", "append_list", "independent_segments", "split_by_time"]
        # mux = [
        #     "-f", "hls",
        #     "-hls_time", f"{self.target.seg_time:.3f}",
        #     "-hls_list_size", str(self.target.list_size),
        #     "-hls_flags", "+".join(hls_flags),
        #     "-hls_segment_filename", seg_pat,
        # ]
        # EVENT 清单：从第 0 片开始不断增长（不删除旧片）
        hls_flags = ["append_list", "independent_segments", "split_by_time"]
        mux = [
            "-f", "hls",
            "-hls_time", f"{self.target.seg_time:.3f}",
            "-hls_list_size", str(self.target.list_size),  # 对 EVENT 影响不大，保留无害
            "-hls_flags", "+".join(hls_flags),
            "-hls_playlist_type", "event",  # 👈 关键：EVENT 模式
            "-hls_segment_filename", seg_pat,
        ]

        if self.target.use_fmp4:
            mux.extend(["-hls_segment_type", "fmp4", "-movflags", "+frag_keyframe+empty_moov"])

        # 输入（rawvideo）从管道进；-r 决定帧时间戳步长
        base = [
            exe, "-hide_banner", "-loglevel", "error", "-y",
            "-f", "rawvideo",
            "-pix_fmt", "rgb24",
            "-s", f"{w}x{h}",
            "-r", f"{fps:.6f}",
            "-i", "pipe:0",
            "-an",
        ]

        gop = max(1, int(round(fps * self.gop_seconds)))

        if self.prefer_nvenc:
            v = [
                "-c:v", "h264_nvenc",
                "-preset", self.preset_nvenc,
                "-tune", self.tune_nvenc,
                "-rc", "constqp", "-qp", str(self.qp),
                "-g", str(gop), "-bf", "2",
                "-pix_fmt", "yuv420p",
                "-profile:v", self.profile,
            ]
        else:
            v = [
                "-c:v", "libx264",
                "-preset", self.preset_x264,
                "-tune", self.tune_x264,
                "-crf", str(self.x264_crf),
                "-g", str(gop), "-bf", "2",
                "-pix_fmt", "yuv420p",
                "-profile:v", self.profile,
                "-x264opts", f"keyint={gop}:min-keyint={gop}",
            ]

        return base + v + mux + [out_m3u8]

    async def _ensure_proc(self, w: int, h: int, fps: float):
        if self._proc and (self._proc.poll() is None):
            return
        # 会话开始清空目录，保证“从头开始播放/索引干净”
        if self.target.cleanup_on_start:
            try:
                if os.path.isdir(self.target.out_dir):
                    shutil.rmtree(self.target.out_dir, ignore_errors=True)
            except Exception:
                pass
        os.makedirs(self.target.out_dir, exist_ok=True)
        cmd = self._build_cmd(w, h, fps)
        creationflags = 0x08000000 if os.name == "nt" else 0  # CREATE_NO_WINDOW
        self._proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            creationflags=creationflags
        )

    def _stop_proc(self):
        if not self._proc:
            return
        try:
            if self._proc.stdin:
                try:
                    self._proc.stdin.close()
                except Exception:
                    pass
            if self._proc.poll() is None:
                if os.name == "nt" and hasattr(signal, "CTRL_BREAK_EVENT"):
                    self._proc.send_signal(signal.CTRL_BREAK_EVENT)
                else:
                    self._proc.terminate()
                try:
                    self._proc.wait(timeout=2.0)
                except Exception:
                    self._proc.kill()
        finally:
            self._proc = None

    # ---------- 自动估计 enc_fps（文件源） ----------
    def _maybe_update_auto_fps(self, pts: float):
        if self.enc_fps is not None:
            return
        if self._last_pts is None:
            self._last_pts = pts
            return
        delta = pts - self._last_pts
        self._last_pts = pts
        if delta <= 0:
            return
        if 1e-6 < delta < 1.0:  # 排除异常大/小
            self._auto_fps_samples.append(1.0 / delta)
        if len(self._auto_fps_samples) >= 12:  # 若采满 12 个样本，取中位数
            med = sorted(self._auto_fps_samples)[len(self._auto_fps_samples)//2]
            # 限制到常见 FPS 档位（30/50/60）
            candidates = [24.0, 25.0, 29.97, 30.0, 50.0, 59.94, 60.0]
            best = min(candidates, key=lambda x: abs(x-med))
            self.enc_fps = best
            self._auto_fps_ready = True

    # ---------- 主循环 ----------
    async def run(self):
        # FIFO 模式，避免“开头帧被 latest 丢弃”
        async with self.bus.subscribe("frames_raw", mode="fifo", maxsize=128) as q:
            w = h = None
            fps_local = self.enc_fps or 30.0  # 初始先用 30，随后若 auto fps 就地重启 ffmpeg
            started = False

            try:
                while not self._stop.is_set():
                    f: Frame = await q.get()
                    if f.camera_id != self.camera_id:
                        continue

                    rgb = f.rgb  # HxWx3, uint8
                    if rgb is None or rgb.ndim != 3 or rgb.shape[2] != 3:
                        continue

                    ih, iw, _ = rgb.shape
                    if self.force_size:
                        W, H = self.force_size
                        if (iw, ih) != (W, H):
                            rgb = cv2.resize(rgb, (W, H), interpolation=cv2.INTER_LINEAR)
                            iw, ih = W, H

                    # 尝试自动估计源 FPS（用于更精准时间戳步长）
                    self._maybe_update_auto_fps(f.pts_in_video)

                    # 若还没启动/需要按 auto-fps 重启 ffmpeg
                    need_restart = False
                    if not started:
                        w, h = iw, ih
                        fps_local = self.enc_fps or fps_local
                        await self._ensure_proc(w, h, fps_local)
                        started = True
                    elif self._auto_fps_ready and (self._proc is not None):
                        # 一次性按自动 FPS 重启，之后锁定
                        self._stop_proc()
                        fps_local = float(self.enc_fps)
                        await self._ensure_proc(w, h, fps_local)
                        self._auto_fps_ready = False

                    if not self._proc or not self._proc.stdin:
                        await asyncio.sleep(0.02)
                        continue

                    try:
                        self._proc.stdin.write(rgb.tobytes())
                    except BrokenPipeError:
                        # ffmpeg 崩了，重启后继续
                        await asyncio.sleep(0.1)
                        self._stop_proc()
                        await self._ensure_proc(w or iw, h or ih, fps_local)
                        continue

                    await asyncio.sleep(0)  # 让出调度
            finally:
                self._stop_proc()

    async def stop(self):
        self._stop.set()
