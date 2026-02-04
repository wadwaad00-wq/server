#!/usr/bin/env python3
# server_ws.py — WebSocket hopper backend (PRINT DEBUG ENABLED)

import asyncio, json, time, os
from urllib.parse import urlparse, parse_qs
import aiohttp
import websockets

PORT = int(os.environ.get("PORT", 3001))

TTL_LOCK     = 60_000
TTL_USED     = 60 * 60 * 1000
MIN_FREE_SLOTS = 1
PAGE_LIMIT   = 100
QUEUE_TARGET = 600
CLEAN_EVERY  = 2.0

ORDERS = ("Asc", "Desc")

def now_ms(): return int(time.time() * 1000)

# --------------------------------------------------
class Universe:
    def __init__(self, uid: str):
        self.uid = uid
        self.min_players = 5
        self.queue = []
        self.locks = {}
        self.used  = {}
        self.seen  = set()
        self.cursor = {"Asc": None, "Desc": None}
        self.delay  = {"Asc": 800,  "Desc": 800}
        self.last_req = {"Asc": 0, "Desc": 0}
        self.scanning = False
        self.waiters = []
        self.pop_lock = asyncio.Lock()

universes = {}

def getU(uid: str) -> Universe:
    if uid not in universes:
        universes[uid] = Universe(uid)
        asyncio.create_task(scanner_loop(universes[uid]))
    return universes[uid]

# --------------------------------------------------
def is_joinable(s, min_players):
    playing = int(s.get("playing", 0))
    maxp = int(s.get("maxPlayers", 0))
    if maxp <= playing: return False
    if playing < min_players: return False
    return True

# --------------------------------------------------
async def fetch_page(session, uid, cursor, order):
    base = f"https://games.roblox.com/v1/games/{uid}/servers/Public"
    url = f"{base}?sortOrder={order}&limit={PAGE_LIMIT}"
    if cursor: url += f"&cursor={cursor}"
    async with session.get(url) as r:
        r.raise_for_status()
        return await r.json()

# --------------------------------------------------
def push_servers(st, arr):
    for sv in arr or []:
        sid = str(sv.get("id") or "")
        if not sid: continue
        if sid in st.seen: continue
        if not is_joinable(sv, st.min_players): continue

        st.queue.append(sid)
        st.seen.add(sid)

        # 🔥 PRINT WHEN DISCOVERED
        print(f"[FOUND] jobId={sid} players={sv.get('playing')}")

# --------------------------------------------------
async def scanner_loop(st):
    if st.scanning: return
    st.scanning = True

    async with aiohttp.ClientSession() as session:
        while True:
            for order in ORDERS:
                try:
                    page = await fetch_page(session, st.uid, st.cursor[order], order)
                    st.cursor[order] = page.get("nextPageCursor")
                    push_servers(st, page.get("data"))
                except Exception as e:
                    print("[SCAN ERROR]", e)
                    await asyncio.sleep(2)
            await asyncio.sleep(0.5)

# --------------------------------------------------
def pop_server(st, who):
    while st.queue:
        sid = st.queue.pop(0)
        if sid in st.used: continue
        st.used[sid] = now_ms()
        print(f"[SEND] jobId={sid} → {who}")
        return sid
    return None

# --------------------------------------------------
async def handle_ws(ws):
    path = ws.request.path if hasattr(ws, "request") else ""
    q = parse_qs(urlparse(path).query)

    uid = q.get("placeId", [None])[0]
    who = q.get("who", ["?"])[0]
    min_players = int(q.get("minPlayers", [5])[0])

    if not uid:
        await ws.close()
        return

    st = getU(uid)
    st.min_players = min_players

    print(f"[WS] CONNECT who={who} placeId={uid}")

    sid = pop_server(st, who)
    if sid:
        await ws.send(json.dumps({"type":"next","id":sid}))

# --------------------------------------------------
async def main():
    print("="*60)
    print("🚀 HOPPER BACKEND (DEBUG PRINT ENABLED)")
    print(f"🌐 Listening on PORT {PORT}")
    print("="*60)

    async with websockets.serve(handle_ws, "0.0.0.0", PORT):
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
