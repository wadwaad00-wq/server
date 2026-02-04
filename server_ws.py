#!/usr/bin/env python3
# server_ws.py — Railway-safe Hopper (HTTP + WS + auto-scan)

import asyncio, json, time, os
from urllib.parse import urlparse, parse_qs

import aiohttp
from aiohttp import web
import websockets

PORT = int(os.environ.get("PORT", 8080))

# 🔴 CHANGE THIS TO YOUR PLACE ID
PLACE_ID = os.environ.get("PLACE_ID", "123456789")
MIN_PLAYERS = int(os.environ.get("MIN_PLAYERS", 5))

PAGE_LIMIT = 100
ORDERS = ("Asc", "Desc")

def now_ms(): return int(time.time() * 1000)

# --------------------------------------------------
class Universe:
    def __init__(self, uid):
        self.uid = uid
        self.min_players = MIN_PLAYERS
        self.queue = []
        self.seen = set()
        self.cursor = {"Asc": None, "Desc": None}
        self.scanning = False

universe = Universe(PLACE_ID)

# --------------------------------------------------
def is_joinable(s):
    playing = int(s.get("playing", 0))
    maxp = int(s.get("maxPlayers", 0))
    return maxp > playing and playing >= universe.min_players

# --------------------------------------------------
async def fetch_page(session, cursor, order):
    base = f"https://games.roblox.com/v1/games/{universe.uid}/servers/Public"
    url = f"{base}?sortOrder={order}&limit={PAGE_LIMIT}"
    if cursor:
        url += f"&cursor={cursor}"
    async with session.get(url) as r:
        r.raise_for_status()
        return await r.json()

# --------------------------------------------------
async def scanner_loop():
    if universe.scanning:
        return
    universe.scanning = True

    print(f"[SCAN] Starting scan for placeId={universe.uid}")

    async with aiohttp.ClientSession() as session:
        while True:
            for order in ORDERS:
                try:
                    page = await fetch_page(session, universe.cursor[order], order)
                    universe.cursor[order] = page.get("nextPageCursor")

                    for sv in page.get("data", []):
                        sid = str(sv.get("id") or "")
                        if not sid or sid in universe.seen:
                            continue
                        if not is_joinable(sv):
                            continue

                        universe.seen.add(sid)
                        universe.queue.append(sid)

                        # ✅ PRINT JOB IDS HERE
                        print(f"[FOUND] jobId={sid} players={sv.get('playing')}")

                except Exception as e:
                    print("[SCAN ERROR]", e)

            await asyncio.sleep(1)

# --------------------------------------------------
async def ws_handler(ws):
    print("[WS] client connected")

    while universe.queue:
        sid = universe.queue.pop(0)
        print(f"[SEND] jobId={sid}")
        await ws.send(json.dumps({"type": "next", "id": sid}))
        break

# --------------------------------------------------
async def http_root(request):
    return web.Response(
        text="Hopper backend running.\nUse WebSocket at /ws\n",
        content_type="text/plain"
    )

# --------------------------------------------------
async def main():
    print("=" * 60)
    print("🚀 HOPPER BACKEND (HTTP + WS)")
    print(f"🌐 Port: {PORT}")
    print(f"🎯 placeId: {PLACE_ID}")
    print(f"👥 minPlayers: {MIN_PLAYERS}")
    print("=" * 60)

    # 🔥 START SCANNER IMMEDIATELY
    asyncio.create_task(scanner_loop())

    # HTTP server (browser-safe)
    app = web.Application()
    app.router.add_get("/", http_root)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()

    # WebSocket server
    await websockets.serve(ws_handler, "0.0.0.0", PORT, path="/ws")

    await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
