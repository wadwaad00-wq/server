#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Roblox JobId Scanner + WebSocket Server
Python 3.10+
pip install aiohttp websockets
"""

import asyncio
import json
import os
import time
from urllib.parse import urlparse, parse_qs

import aiohttp
import websockets
from aiohttp import web

# =====================================================
# CONFIG
# =====================================================

PLACE_ID = os.getenv("PLACE_ID", "109983668079237")  # change or set in Railway
MIN_PLAYERS = int(os.getenv("MIN_PLAYERS", "5"))
PORT = int(os.getenv("PORT", "8080"))

SCAN_DELAY = 1.2
PAGE_LIMIT = 100

# =====================================================
# STATE
# =====================================================

queue: list[str] = []
seen: set[str] = set()
clients: set[websockets.WebSocketServerProtocol] = set()

# =====================================================
# HELPERS
# =====================================================

def now():
    return int(time.time())

def is_joinable(server: dict) -> bool:
    playing = int(server.get("playing", 0))
    maxp = int(server.get("maxPlayers", 0))
    if playing < MIN_PLAYERS:
        return False
    if maxp <= playing:
        return False
    if server.get("vipServerId") or server.get("privateServerId"):
        return False
    return True

# =====================================================
# ROBLOX SCANNER
# =====================================================

async def scan_servers():
    print(f"[SCAN] Starting scan for placeId={PLACE_ID}")

    url_base = f"https://games.roblox.com/v1/games/{PLACE_ID}/servers/Public"
    cursor = None

    async with aiohttp.ClientSession() as session:
        while True:
            params = {
                "limit": PAGE_LIMIT,
                "sortOrder": "Desc",
                "excludeFullGames": "true",
            }
            if cursor:
                params["cursor"] = cursor

            try:
                async with session.get(url_base, params=params) as r:
                    data = await r.json()
            except Exception as e:
                print("[ERROR] Roblox API:", e)
                await asyncio.sleep(3)
                continue

            for s in data.get("data", []):
                if not is_joinable(s):
                    continue

                job_id = s.get("id")
                if not job_id or job_id in seen:
                    continue

                seen.add(job_id)
                queue.append(job_id)

                # 🔥 PRINT JOB ID
                print(f"[FOUND] jobId={job_id} players={s.get('playing')}")

                # Push to all connected WS clients
                for ws in list(clients):
                    try:
                        await ws.send(json.dumps({"type": "job", "id": job_id}))
                    except:
                        clients.discard(ws)

            cursor = data.get("nextPageCursor")
            if not cursor:
                await asyncio.sleep(SCAN_DELAY)

# =====================================================
# WEBSOCKET HANDLER (MODERN)
# =====================================================

async def ws_handler(ws):
    # IMPORTANT: filter path manually
    if ws.path != "/ws":
        await ws.close(code=1008, reason="Invalid path")
        return

    clients.add(ws)
    print("[WS] client connected")

    # Send backlog
    for job in queue[-25:]:
        await ws.send(json.dumps({"type": "job", "id": job}))

    try:
        async for _ in ws:
            pass
    finally:
        clients.discard(ws)
        print("[WS] client disconnected")

# =====================================================
# HTTP SERVER (FOR BROWSER)
# =====================================================

async def http_index(_):
    return web.Response(
        text="""
        <html>
        <body>
        <h2>Roblox JobId Scanner</h2>
        <pre id="out"></pre>
        <script>
        const ws = new WebSocket((location.protocol === "https:" ? "wss://" : "ws://") + location.host + "/ws");
        ws.onmessage = e => {
            const d = JSON.parse(e.data);
            document.getElementById("out").textContent += d.id + "\\n";
        };
        </script>
        </body>
        </html>
        """,
        content_type="text/html"
    )

# =====================================================
# MAIN
# =====================================================

async def main():
    print("=" * 60)
    print("🚀 HOPPER BACKEND (HTTP + WS)")
    print(f"🌐 Port: {PORT}")
    print(f"🎯 placeId: {PLACE_ID}")
    print(f"👥 minPlayers: {MIN_PLAYERS}")
    print("=" * 60)

    # HTTP app
    app = web.Application()
    app.router.add_get("/", http_index)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()

    # WebSocket server (NO path argument)
    await websockets.serve(ws_handler, "0.0.0.0", PORT)

    # Scanner
    await scan_servers()

if __name__ == "__main__":
    asyncio.run(main())
