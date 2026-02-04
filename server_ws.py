#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# server_ws.py — WebSocket hopper backend (PROVEN - MINIMUM 5 PLAYERS)
# Python 3.10+ ; pip install websockets aiohttp

import asyncio
import json
import time
import os
from urllib.parse import urlparse, parse_qs

import aiohttp
import websockets

# =========================
# CONFIG
# =========================
TTL_LOCK        = 60_000            # 60s soft lock (during teleport)
TTL_USED        = 60 * 60 * 1000    # 1h hard lock (no duplicates)
MIN_FREE_SLOTS  = 1
PAGE_LIMIT      = 100
QUEUE_TARGET    = 600
CLEAN_EVERY     = 2.0
STALL_MS        = 120_000

ORDERS = ("Asc", "Desc")

def now_ms():
    return int(time.time() * 1000)

# =========================
# STATE
# =========================
class Universe:
    __slots__ = (
        "uid","min_players","queue","locks","used","seen",
        "cursor","delay","last_req","scanning","waiters",
        "last_progress","pop_lock"
    )

    def __init__(self, uid: str):
        self.uid = uid
        self.min_players = 5
        self.queue = []
        self.locks = {}
        self.used  = {}
        self.seen  = set()
        self.cursor = {"Asc": None, "Desc": None}
        self.delay  = {"Asc": 800, "Desc": 800}
        self.last_req = {"Asc": 0, "Desc": 0}
        self.scanning = False
        self.waiters = []
        self.last_progress = now_ms()
        self.pop_lock = asyncio.Lock()

universes = {}

def getU(uid: str) -> Universe:
    uid = str(uid)
    if uid not in universes:
        st = Universe(uid)
        universes[uid] = st
        asyncio.create_task(scanner_loop(st))
    return universes[uid]

# =========================
# FILTERS
# =========================
def is_joinable_base(s):
    playing = int(s.get("playing") or s.get("playerCount") or 0)
    maxp = int(s.get("maxPlayers") or s.get("maxPlayerCount") or 0)
    if not maxp or playing >= maxp:
        return False
    if s.get("vipServerId") or s.get("privateServerId"):
        return False
    return str(s.get("serverType", "public")).lower() == "public"

def is_joinable_with_min(s, min_players):
    if not is_joinable_base(s):
        return False
    playing = int(s.get("playing") or s.get("playerCount") or 0)
    maxp = int(s.get("maxPlayers") or s.get("maxPlayerCount") or 0)
    if playing < min_players:
        return False
    if (maxp - playing) < MIN_FREE_SLOTS:
        return False
    return True

# =========================
# ROBLOX API
# =========================
async def fetch_page(session, uid, cursor, order):
    base = f"https://games.roblox.com/v1/games/{uid}/servers/Public"
    url = f"{base}?sortOrder={order}&limit={PAGE_LIMIT}&excludeFullGames=true"
    if cursor:
        url += f"&cursor={cursor}"

    async with session.get(url) as r:
        if r.status == 429:
            raise RuntimeError("RATE_LIMIT")
        r.raise_for_status()
        return await r.json()

# =========================
# QUEUE HANDLING
# =========================
def push_servers(st, data):
    pushed = 0
    for sv in data or []:
        if not is_joinable_with_min(sv, st.min_players):
            continue

        sid = str(sv.get("id") or sv.get("jobId") or "")
        if not sid:
            continue
        if sid in st.seen or sid in st.used or sid in st.locks:
            continue

        st.queue.append(sid)
        st.seen.add(sid)
        pushed += 1

    if pushed:
        print(f"[SCAN] +{pushed} | queue={len(st.queue)}")
    return pushed

def try_pop_available(st, current_job, who):
    while st.queue:
        sid = st.queue.pop(0)

        if current_job and sid == current_job:
            continue
        if sid in st.used or sid in st.locks:
            continue

        ts = now_ms()
        st.locks[sid] = ts
        st.used[sid]  = ts

        print(f"[SEND] {sid} → {who} (min={st.min_players})")
        return sid
    return None

def drain_waiters(st):
    if not st.waiters or not st.queue:
        return

    remaining = []
    for fut, job, who in st.waiters:
        if fut.done():
            continue
        sid = try_pop_available(st, job, who)
        if sid:
            fut.set_result(sid)
        else:
            remaining.append((fut, job, who))
    st.waiters = remaining

# =========================
# SCANNER
# =========================
async def scan_tick(st, order, session):
    now = now_ms()
    if now - st.last_req[order] < st.delay[order]:
        await asyncio.sleep((st.delay[order] - (now - st.last_req[order])) / 1000)

    st.last_req[order] = now_ms()

    try:
        page = await fetch_page(session, st.uid, st.cursor[order], order)
        st.cursor[order] = page.get("nextPageCursor")
        pushed = push_servers(st, page.get("data"))
        if pushed:
            st.last_progress = now_ms()
            drain_waiters(st)
    except Exception:
        st.delay[order] = min(4000, int(st.delay[order] * 1.5))

async def scanner_loop(st):
    if st.scanning:
        return
    st.scanning = True

    async with aiohttp.ClientSession() as session:
        while True:
            await asyncio.gather(
                scan_tick(st, "Asc", session),
                scan_tick(st, "Desc", session),
            )

# =========================
# CLEANER
# =========================
async def cleaner():
    while True:
        await asyncio.sleep(CLEAN_EVERY)
        now = now_ms()
        for st in universes.values():
            st.locks = {k:v for k,v in st.locks.items() if now - v < TTL_LOCK}
            st.used  = {k:v for k,v in st.used.items() if now - v < TTL_USED}
            drain_waiters(st)

# =========================
# WEBSOCKET
# =========================
async def ws_send_next(st, ws, current_job, who):
    async with st.pop_lock:
        sid = try_pop_available(st, current_job, who)

    if sid:
        await ws.send(json.dumps({"type": "next", "id": sid}))
        return

    fut = asyncio.get_event_loop().create_future()
    st.waiters.append((fut, current_job, who))

    try:
        sid = await asyncio.wait_for(fut, timeout=10)
        await ws.send(json.dumps({"type": "next", "id": sid}))
    except asyncio.TimeoutError:
        pass

async def handle_ws(ws):
    path = ws.request.path if hasattr(ws, "request") else "/ws"
    q = {k:v[0] for k,v in parse_qs(urlparse(path).query).items()}

    uid = q.get("placeId") or q.get("uid")
    who = q.get("who", "?")
    min_players = int(q.get("minPlayers", 5))

    if not uid:
        await ws.send(json.dumps({"type":"error","error":"no placeId"}))
        return

    st = getU(uid)
    st.min_players = min_players

    print(f"[WS] + {who} uid={uid} min={min_players}")
    current_job = None

    await ws_send_next(st, ws, current_job, who)

    async for raw in ws:
        msg = json.loads(raw)
        t = msg.get("type")

        if t == "next":
            current_job = msg.get("currentJob")
            await ws_send_next(st, ws, current_job, who)
        elif t == "release":
            sid = msg.get("id")
            if sid:
                st.locks.pop(sid, None)

# =========================
# MAIN (Railway-safe)
# =========================
async def main():
    PORT = int(os.environ.get("PORT", 3001))

    print("="*60)
    print("🚀 ROBLOX HOPPER BACKEND (Railway Ready)")
    print(f"🌐 Port: {PORT}")
    print("👥 Min Players Default: 5")
    print("🔒 No duplicates for 1 hour")
    print("="*60)

    asyncio.create_task(cleaner())

    async with websockets.serve(handle_ws, "0.0.0.0", PORT):
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
