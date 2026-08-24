"""
main.py — API "Trạng thái Server" cho website TuyTam Community
Deploy Render. KHÔNG dùng MongoDB nữa — cache toàn bộ trong bộ nhớ (in-memory TTL),
vì dữ liệu chỉ là số liệu hiển thị real-time, không cần lưu lâu dài.

Cung cấp 2 nguồn dữ liệu cho trang chủ:
  1. Discord  — tổng member + đang online, qua Discord Invite API công khai
                (KHÔNG cần bot token).
  2. DonutSMP — server online/offline + số người chơi hiện tại, qua
                mcsrvstat.us (ping Minecraft server công khai, KHÔNG cần API key).

Biến môi trường (đều có default, không bắt buộc set trên Render):
  DISCORD_INVITE_CODE   mã invite Discord, mặc định "FkC45DwtyN"
                         -> XÁC NHẬN LẠI mã này đúng với discord.gg/... hiện tại
  DONUTSMP_HOST          địa chỉ server DonutSMP, mặc định "donutsmp.net"
  CACHE_TTL_SECONDS      thời gian cache mỗi nguồn dữ liệu (giây), mặc định 60
"""

import asyncio
import os
import time
from typing import Awaitable, Callable, Optional

import httpx
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# ══════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════
DISCORD_INVITE_CODE = os.getenv("DISCORD_INVITE_CODE", "FkC45DwtyN")
DONUTSMP_HOST = os.getenv("DONUTSMP_HOST", "donutsmp.net")
CACHE_TTL = int(os.getenv("CACHE_TTL_SECONDS", "60"))

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


@app.get("/status/all")
async def status_all():
    discord, donutsmp = await asyncio.gather(
        _cached("discord", _fetch_discord),
        _cached("donutsmp", _fetch_donutsmp),
    )
    return {"discord": discord, "donutsmp": donutsmp}


@app.get("/health")
def health():
    return {"status": "ok"}
