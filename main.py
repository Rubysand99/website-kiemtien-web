"""
main.py — API "Trạng thái Server" cho website TuyTam Community
Deploy Render.

Cung cấp 3 nguồn dữ liệu cho trang chủ:
  1. Discord  — tổng member + đang online, qua Discord Invite API công khai
                (KHÔNG cần bot token). Cache in-memory.
  2. DonutSMP — server online/offline + số người chơi hiện tại, qua
                mcsrvstat.us (ping Minecraft server công khai, KHÔNG cần API key).
                Cache in-memory.
  3. Feed     — tin nhắn mới nhất ở kênh thông báo/legit, do bot Rudy ghi vào
                MongoDB collection "site_feed" (xem core/site_feed.py bên repo bot).
                Đọc-only ở đây, KHÔNG ghi gì vào Mongo.

Biến môi trường:
  DISCORD_INVITE_CODE   mã invite Discord, mặc định "FkC45DwtyN" (có default, không bắt buộc set)
  DONUTSMP_HOST          địa chỉ server DonutSMP, mặc định "donutsmp.net" (có default)
  CACHE_TTL_SECONDS      thời gian cache mỗi nguồn dữ liệu (giây), mặc định 60 (có default)
  MONGO_URI              connection string Mongo Atlas — BẮT BUỘC set trên Render để mục
                         Feed hoạt động (dùng CHUNG cụm Mongo với bot). Thiếu biến này thì
                         /status/discord và /status/donutsmp vẫn chạy bình thường, chỉ
                         riêng feed trả về rỗng — KHÔNG làm sập cả service.
"""

import asyncio
import os
import time
from typing import Awaitable, Callable, Optional

import httpx
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient

# ══════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════
DISCORD_INVITE_CODE = os.getenv("DISCORD_INVITE_CODE", "FkC45DwtyN")
DONUTSMP_HOST = os.getenv("DONUTSMP_HOST", "donutsmp.net")
CACHE_TTL = int(os.getenv("CACHE_TTL_SECONDS", "60"))
MONGO_URI = os.getenv("MONGO_URI")
FEED_LIMIT = int(os.getenv("FEED_LIMIT", "6"))

# mcsrvstat.us yêu cầu User-Agent không rỗng, thiếu sẽ bị 403
_UA = f"TuyTamCommunityStatus/1.0 (+https://discord.gg/{DISCORD_INVITE_CODE})"

# ══════════════════════════════════════════
# CACHE — in-memory, đủ dùng cho dữ liệu ngắn hạn này
# ══════════════════════════════════════════
_cache: dict[str, tuple[float, dict]] = {}


async def _cached(key: str, fetch_fn: Callable[[], Awaitable[dict]]) -> dict:
    now = time.time()
    hit = _cache.get(key)
    if hit and (now - hit[0]) < CACHE_TTL:
        return hit[1]
    data = await fetch_fn()
    _cache[key] = (now, data)
    return data


# ══════════════════════════════════════════
# FETCHERS
# ══════════════════════════════════════════
async def _fetch_discord() -> dict:
    url = f"https://discord.com/api/v10/invites/{DISCORD_INVITE_CODE}?with_counts=true"
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            res = await client.get(url)
        if res.status_code != 200:
            return {"online": False, "error": f"http_{res.status_code}"}
        data = res.json()
        guild = data.get("guild") or {}
        return {
            "online": True,
            "guild_name": guild.get("name"),
            "member_count": data.get("approximate_member_count"),
            "online_count": data.get("approximate_presence_count"),
            "invite_url": f"https://discord.gg/{DISCORD_INVITE_CODE}",
        }
    except Exception as e:
        return {"online": False, "error": str(e)}


async def _fetch_donutsmp() -> dict:
    url = f"https://api.mcsrvstat.us/3/{DONUTSMP_HOST}"
    try:
        async with httpx.AsyncClient(timeout=6, headers={"User-Agent": _UA}) as client:
            res = await client.get(url)
        data = res.json()
        online = bool(data.get("online"))
        players = data.get("players") or {}
        return {
            "online": online,
            "players_online": players.get("online") if online else None,
            "players_max": players.get("max") if online else None,
            "host": DONUTSMP_HOST,
        }
    except Exception as e:
        return {"online": False, "error": str(e)}


# ══════════════════════════════════════════
# MONGO — chỉ dùng để ĐỌC feed do bot Rudy ghi vào (collection "site_feed").
# Không set MONGO_URI vẫn chạy được — feed trả rỗng thay vì làm sập cả service.
# ══════════════════════════════════════════
_mongo_client: Optional[AsyncIOMotorClient] = None


def _get_feed_col():
    global _mongo_client
    if not MONGO_URI:
        return None
    if _mongo_client is None:
        _mongo_client = AsyncIOMotorClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    return _mongo_client["tuytam_bot"]["site_feed"]


def _serialize_feed_doc(d: dict) -> dict:
    ts = d.get("timestamp")
    return {
        "author_name": d.get("author_name"),
        "author_avatar": d.get("author_avatar"),
        "content": d.get("content"),
        "attachment_url": d.get("attachment_url"),
        "jump_url": d.get("jump_url"),
        "timestamp": ts.isoformat() if ts else None,
    }


async def _fetch_feed() -> dict:
    col = _get_feed_col()
    if col is None:
        return {"thongbao": [], "legit": [], "error": "mongo_not_configured"}
    try:
        result = {}
        for label in ("thongbao", "legit"):
            cursor = (
                col.find({"channel_label": label})
                .sort("timestamp", -1)
                .limit(FEED_LIMIT)
            )
            docs = [d async for d in cursor]
            result[label] = [_serialize_feed_doc(d) for d in docs]
        return result
    except Exception as e:
        return {"thongbao": [], "legit": [], "error": str(e)}


# ══════════════════════════════════════════
# APP
# ══════════════════════════════════════════
app = FastAPI(title="TuyTam Community Status API", docs_url=None, redoc_url=None)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.get("/status/discord")
async def status_discord():
    return await _cached("discord", _fetch_discord)


@app.get("/status/donutsmp")
async def status_donutsmp():
    return await _cached("donutsmp", _fetch_donutsmp)


@app.get("/status/feed")
async def status_feed():
    return await _cached("feed", _fetch_feed)


@app.get("/status/all")
async def status_all():
    discord, donutsmp, feed = await asyncio.gather(
        _cached("discord", _fetch_discord),
        _cached("donutsmp", _fetch_donutsmp),
        _cached("feed", _fetch_feed),
    )
    return {"discord": discord, "donutsmp": donutsmp, "feed": feed}


@app.get("/health")
def health():
    return {"status": "ok"}
