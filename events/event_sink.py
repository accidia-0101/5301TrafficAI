# event_sink.py
"""
事件汇聚器（去抖 + 冷却 + 可选DB入库）

功能：
- 订阅 'detections'（Detection: accident/weather）
- 对 'accident' 做两级阈值去抖（arm_thresh / fire_thresh + 连续帧计数）
- 触发后进入冷却期，避免同一事故重复上报
- 默认打印；若设置环境变量 TRAFFICAI_PG_DSN 且安装 asyncpg，则自动入库

PostgreSQL 预期表结构（与项目报告一致，略）：
  CREATE EXTENSION IF NOT EXISTS pgcrypto;
  CREATE TABLE IF NOT EXISTS cameras(
    camera_id VARCHAR(64) PRIMARY KEY,
    location_name TEXT
  );
  CREATE TYPE event_type AS ENUM('accident','weather');      -- 如果不想用 ENUM，改成 TEXT 也可以
  CREATE TYPE weather_type AS ENUM('clear','rain','fog');    -- 同上
  CREATE TABLE IF NOT EXISTS events(
    event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    timestamp TIMESTAMPTZ NOT NULL,
    camera_id VARCHAR(64) REFERENCES cameras(camera_id),
    type event_type NOT NULL,
    weather weather_type,
    confidence REAL CHECK (confidence >= 0 AND confidence <= 1)
  );
  CREATE TABLE IF NOT EXISTS event_text(
    event_id UUID PRIMARY KEY REFERENCES events(event_id),
    evidence_text TEXT NOT NULL
  );
"""

from __future__ import annotations
import os
import asyncio
from dataclasses import dataclass
from typing import Optional, Dict

from bus import Detection, AsyncBus


# ---------------- 参数（可按需调整） ----------------
ARM_THRESH: float = 0.60        # 进入“疑似”门槛
FIRE_THRESH: float = 0.80       # 真正触发门槛
REQUIRE_CONSEC: int = 2         # 连续命中帧数（>= ARM 且 happened=True）
COOLDOWN_SEC: float = 8.0       # 同一相机冷却期


# ---------------- 可选：PostgreSQL 写入器 ----------------
class PgWriter:
    def __init__(self, dsn: str):
        self._dsn = dsn
        self._pool = None
        self.enabled = False

    async def start(self):
        try:
            import asyncpg
            self._pool = await asyncpg.create_pool(dsn=self._dsn, min_size=1, max_size=4)
            self.enabled = True
            print("✅ PostgreSQL sink enabled.")
        except Exception as e:
            print(f"⚠️  PostgreSQL sink disabled: {e}")
            self.enabled = False

    async def stop(self):
        if self._pool:
            await self._pool.close()
            self._pool = None
            self.enabled = False

    async def insert_event(self, det: Detection, evidence_text: str) -> Optional[str]:
        """
        插入 events + event_text；返回 event_id（字符串）或 None
        说明：
        - 假定已存在 cameras(camera_id)；若没有你可以事先插入一条
        - 若你未使用 ENUM，可把 SQL 里的类型直接当 TEXT 传
        """
        if not self.enabled or not self._pool:
            return None

        try:
            async with self._pool.acquire() as conn:
                event_id = await conn.fetchval(
                    """
                    INSERT INTO events (timestamp, camera_id, type, weather, confidence)
                    VALUES (to_timestamp($1), $2, $3, $4, $5)
                    RETURNING event_id
                    """,
                    det.ts_unix, det.camera_id, 'accident', None, det.confidence
                )
                await conn.execute(
                    """
                    INSERT INTO event_text (event_id, evidence_text)
                    VALUES ($1, $2)
                    """,
                    event_id, evidence_text
                )
                return str(event_id)
        except Exception as e:
            print(f"⚠️  DB insert failed: {e}")
            return None


# ---------------- 内部状态 ----------------
@dataclass
class _CamState:
    consec_hits: int = 0
    last_fire_ts: float = 0.0


# ---------------- 事件文本生成 ----------------
def build_evidence_text(det: Detection) -> str:
    # 简单可读；后续你可以把检测框、截图哈希等证据拼进来
    return (f"[accident] camera={det.camera_id} "
            f"ts={det.ts_unix:.3f} "
            f"conf={det.confidence:.3f}")


# ---------------- 上报（打印 + 可选入库） ----------------
async def on_event(det: Detection, pg: Optional[PgWriter]):
    text = build_evidence_text(det)
    print(f"🚨 [ALERT] {text}")
    if pg and pg.enabled:
        event_id = await pg.insert_event(det, text)
        if event_id:
            print(f"🗄️  saved to DB event_id={event_id}")


# ---------------- 主协程：事件去抖/冷却 ----------------
async def run_event_sink(
    bus: AsyncBus,
    *,
    arm_thresh: float = ARM_THRESH,
    fire_thresh: float = FIRE_THRESH,
    require_consecutive: int = REQUIRE_CONSEC,
    cooldown_sec: float = COOLDOWN_SEC,
):
    """
    订阅 'detections'：
      - 仅处理 type='accident'
      - 连续帧 >= require_consecutive 且 conf >= fire_thresh 时触发一次
      - 触发后进入 cooldown_sec 冷却期
    """
    q = bus.subscribe("detections")
    states: Dict[str, _CamState] = {}

    # 可选：初始化 PostgreSQL sink
    pg: Optional[PgWriter] = None
    dsn = os.getenv("TRAFFICAI_PG_DSN", "").strip()
    if dsn:
        pg = PgWriter(dsn)
        await pg.start()

    try:
        while True:
            det: Detection = await q.get()

            if det.type != "accident":
                # 如需也处理 weather，可在此扩展
                continue

            st = states.setdefault(det.camera_id, _CamState())

            # 冷却期内，直接忽略
            if (det.ts_unix - st.last_fire_ts) < cooldown_sec:
                continue

            # 去抖与计数：同时要求 happened=True
            if det.happened and det.confidence >= arm_thresh:
                st.consec_hits += 1
            else:
                st.consec_hits = 0

            if det.happened and det.confidence >= fire_thresh and st.consec_hits >= require_consecutive:
                st.last_fire_ts = det.ts_unix
                st.consec_hits = 0
                await on_event(det, pg)

            await asyncio.sleep(0)
    finally:
        if pg:
            await pg.stop()
