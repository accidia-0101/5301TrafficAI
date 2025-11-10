
# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
from typing import List, Optional

from events.bus import AsyncBus
from events.frame_discrete import run_frame_source_raw, run_sampler_equal_time


class SingleFileSession:
    """
    单路视频检测会话：
    - frames_raw → 等时采样 → frames → YOLO → accident → aggregator → open/close
    - 视频播完自动 flush 结案
    """

    def __init__(
        self,
        *,
        camera_id: str,
        file_path: str,
        bus: AsyncBus,
        sampler_fps: float = 15.0,
        session_id: Optional[str] = None,
    ) -> None:
        self.camera_id = camera_id
        self.file_path = file_path
        self.bus = bus
        self.sampler_fps = sampler_fps
        self.session_id = session_id or f"sess-{camera_id}"
        self._tasks: List[asyncio.Task] = []
        self._running = False

    # --------------------------
    # 启动该摄像头任务
    # --------------------------
    # def start(self, *, loop) -> None:
    #     if self._running:
    #         return
    #
    #     async def _start():
    #         try:
    #             self._running = True
    #
    #             # ✅ 实例化聚合器（每路独立）
    #             aggregator = AccidentAggregator(
    #                 camera_id=self.camera_id,
    #                 bus=self.bus,
    #                 session_id=self.session_id,
    #             )
    #
    #             # ✅ 启动三个主要任务
    #             t1 = asyncio.create_task(run_frame_source_raw(self.bus, self.camera_id, self.file_path))
    #             t2 = asyncio.create_task(run_sampler_equal_time(self.bus, self.camera_id))
    #             t3 = asyncio.create_task(aggregator.run())
    #             self._tasks = [t1, t2, t3]
    #             print(f"[session] started {self.camera_id}")
    #
    #             # ✅ 等待帧源播完（文件视频）
    #             await t1
    #
    #             # ✅ 帧源结束 → 主动结案
    #             await aggregator.flush()
    #
    #         except asyncio.CancelledError:
    #             pass
    #         except Exception as e:
    #             print(f"[session] error {self.camera_id}: {e}")
    #         finally:
    #             self._running = False
    #             print(f"[session] finished {self.camera_id}")
    #
    #     asyncio.run_coroutine_threadsafe(_start(), loop)
    def start(self, *, loop) -> None:
        if self._running:
            return

        async def _start():
            from events.Accident_detect.incident_aggregator import AccidentAggregator
            self._running = True

            aggregator = AccidentAggregator(
                camera_id=self.camera_id,
                bus=self.bus,
                session_id=self.session_id,
            )

            # 启动帧源、采样器、聚合器
            t_src = asyncio.create_task(run_frame_source_raw(self.bus, self.camera_id, self.file_path))
            t_smp = asyncio.create_task(run_sampler_equal_time(self.bus, self.camera_id))
            t_agg = asyncio.create_task(aggregator.run())
            self._tasks = [t_src, t_smp, t_agg]
            print(f"[session] started {self.camera_id}")

            try:
                # 等视频播放结束
                await t_src
                print(f"[frame_source] {self.camera_id} finished, releasing video")

                # 停止采样器，防止继续推送帧
                t_smp.cancel()
                await asyncio.gather(t_smp, return_exceptions=True)

                # ✅ 加排空窗口：给检测器和聚合器 0.5～1s 时间消化在途帧
                await asyncio.sleep(0.8)

                # ✅ 再 flush 结案
                await aggregator.flush()

                # 最后安全取消聚合器（它是长循环）
                t_agg.cancel()
                await asyncio.gather(t_agg, return_exceptions=True)

            except asyncio.CancelledError:
                try:
                    await aggregator.flush()
                except Exception:
                    pass
            except Exception as e:
                print(f"[session] error {self.camera_id}: {e}")
            finally:
                self._running = False
                print(f"[session] finished {self.camera_id}")

        asyncio.run_coroutine_threadsafe(_start(), loop)

    # --------------------------
    # 停止该摄像头任务
    # --------------------------
    def stop(self, *, loop) -> None:
        if not self._running:
            return

        async def _stop():
            for t in self._tasks:
                t.cancel()
            await asyncio.gather(*self._tasks, return_exceptions=True)
            self._tasks.clear()
            self._running = False
            print(f"[session] stopped {self.camera_id}")

        asyncio.run_coroutine_threadsafe(_stop(), loop)
