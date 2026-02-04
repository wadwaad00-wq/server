#!/usr/bin/env python3
# Python 3.10+
# pip install websockets aiohttp

import asyncio
import json
import time
import os
from urllib.parse import urlparse, parse_qs

import aiohttp
import websockets
from websockets.http import Headers

# =========================
# CONFIG
# =========================
TTL_LOCK = 60_000
TTL_USED = 60 * 60 * 1000
MIN_FREE_SLOTS = 1
PAGE_LIMIT = 100
QUEUE_TARGET = 600
CLEAN_EVERY = 2.0

ORDERS = ("Asc", "Desc")

def now_ms():
    return int(time.time() * 1000)

# =========================
# STATE
# =========================
class Universe:
    def __init__(self, uid):
        self.uid = uid
        self.min_players = 5
        self.queue = []
        self.locks = {}
        self.used = {}
        self.seen = set()
        self.cursor = {"Asc": None, "Desc": None}
        self.delay = {"Asc": 800, "Desc": 800}
        self.last_req = {"Asc": 0, "Desc": 0}
        self.scanning = False
        self.waiters = []
        self.last_progress = now_ms()
        self.pop_lock = asyncio.Lock()

universes = {}

def getU(uid):
    if uid not in universes:
        universes[uid] = Universe(uid)
        asyncio.create_task(scanner_loop(universes[uid]))
    return universes[uid]

# =========================
# FILTERS
# =========================
def is_joinable(s, min_players):
    playing = int(s.get("playing", 0))
    maxp = int(s.get("maxPlayers", 0))
    if not maxp or playing >= maxp:
        return False
    if playing < min_players:
        return False
    if (maxp - playing) < MIN_FREE_SLOTS:
        return False
    if s.get("vipServerId") or s.get("privateServerId"):
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
        r.raise_for_status()
        return await r.json()

# =========================
# QUEUE
# =========================
def push_servers(st, data):
    added = 0
    for s in data or []:
        if not is_joinable(s, st.min_players):
            continue

        sid = str(s.get("id"))
        if sid in st.seen or sid in st.used or sid in st.locks:
            continue

        st.queue.append(sid)
        st.seen.add(sid)
        added += 1

    if added:
        print(f"[SCAN] +{added} servers | queue={len(st.queue)}")

def try_pop(st, current_job, who):
    while st.queue:
        sid = st.queue.pop(0)
        if sid == current_job:
            continue
        if sid in st.used or sid in st.locks:
            continue

        ts = now_ms()
        st.locks[sid] = ts
        st.used[sid] = ts
        print(f"[SEND] {sid} → {who}")
        return sid
    return None

# =========================
# SCANNER
# =========================
async def scan_tick(st, order, session):
    try:
        page = await fetch_page(session, st.uid, st.cursor[order], order)
        st.cursor[order] = page.get("nextPageCursor")
        push_servers(st, page.get("data"))
    except Exception:
        await asyncio.sleep(1)

async def scanner_loop(st):
    if st.scanning:
        return
    st.scanning = True
    async with aiohttp.ClientSession() as session:
        while True:
            await asyncio.gather(
                scan_tick(st, "Asc", session),
                scan_tick(st, "Desc", session)
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
            st.used = {k:v for k,v in st.used.items() if now - v < TTL_USED}

# =========================
# HTTP HANDLER (BROWSER)
# =========================
async def process_request(path, headers):
    if path == "/" or path.startswith("/health"):
        body = f"""
        <html>
        <body style="font-family:Arial">
        <h2>🚀 Roblox Hopper Backend</h2>
        <p>Status: <b>RUNNING</b></p>
        <p>Universes loaded: {len(universes)}</p>
        </body>
        </html>
        """.encode()

        return (
            200,
            Headers({"Content-Type": "text/html"}),
            body
        )
    return None  # allow websocket upgrade

# =========================
# WEBSOCKET
# =========================
async def handle_ws(ws):
    path = ws.request.path
    q = {k:v[0] for k,v in parse_qs(urlparse(path).query).items()}

    uid = q.get("placeId")
    who = q.get("who", "client")
    min_players = int(q.get("minPlayers", 5))

    if not uid:
        await ws.send(json.dumps({"error":"no placeId"}))
        return

    st = getU(uid)
    st.min_players = min_players

    print(f"[WS] + {who} ({uid})")

    async with st.pop_lock:
        sid = try_pop(st, None, who)

    if sid:
        await ws.send(json.dumps({"type":"next","id":sid}))

    async for raw in ws:
        msg = json.loads(raw)
        if msg.get("type") == "next":
            async with st.pop_lock:
                sid = try_pop(st, msg.get("currentJob"), who)
            if sid:
                await ws.send(json.dumps({"type":"next","id":sid}))

# =========================
# MAIN
# =========================
async def main():
    PORT = int(os.environ.get("PORT", 8080))

    print("="*60)
    print("🚀 ROBLOX HOPPER BACKEND (Browser + WS)")
    print(f"🌐 Port: {PORT}")
    print("="*60)

    asyncio.create_task(cleaner())

    async with websockets.serve(
        handle_ws,
        "0.0.0.0",
        PORT,
        process_request=process_request
    ):
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
