import asyncio
import json
import os
import time
from urllib.parse import parse_qs

import websockets
from websockets.server import ServerConnection

PORT = int(os.environ.get("PORT", 8080))

USED_IDS = {}
USED_TTL = 3600  # 1 hour

print("=" * 60)
print("🚀 ROBLOX HOPPER BACKEND (Browser + WS)")
print(f"🌐 Port: {PORT}")
print("=" * 60)


# --------------------------------------------------
# CLEANUP USED IDS
# --------------------------------------------------
def cleanup_used():
    now = time.time()
    for k in list(USED_IDS):
        if now - USED_IDS[k] > USED_TTL:
            del USED_IDS[k]


# --------------------------------------------------
# HTTP HANDLER (Browser / Railway health checks)
# --------------------------------------------------
async def process_request(conn: ServerConnection, request):
    path = conn.path  # ✅ CORRECT WAY

    if path == "/" or path.startswith("/health"):
        body = (
            "🚀 Roblox Hopper Backend\n"
            "Status: RUNNING\n"
            "WebSocket endpoint: /ws\n"
        ).encode()

        return (
            200,
            [
                ("Content-Type", "text/plain"),
                ("Content-Length", str(len(body))),
            ],
            body,
        )

    return None  # allow WebSocket upgrades


# --------------------------------------------------
# WEBSOCKET HANDLER
# --------------------------------------------------
async def ws_handler(ws):
    query = parse_qs(ws.request.path.split("?", 1)[1] if "?" in ws.request.path else "")
    place_id = query.get("placeId", [None])[0]
    who = query.get("who", ["unknown"])[0]

    if not place_id:
        await ws.close(code=1008, reason="Missing placeId")
        return

    print(f"[WS] + {who} ({place_id})")

    try:
        while True:
            cleanup_used()

            # 🔧 DEMO JOB ID (replace with real Roblox API later)
            job_id = f"job_{int(time.time()*1000)}"

            if job_id in USED_IDS:
                await asyncio.sleep(1)
                continue

            USED_IDS[job_id] = time.time()

            payload = {
                "type": "next",
                "placeId": place_id,
                "jobId": job_id,
            }

            await ws.send(json.dumps(payload))
            print(f"[SEND] {job_id} → {who}")

            await asyncio.sleep(2)

    except websockets.exceptions.ConnectionClosed:
        print(f"[WS] - {who}")


# --------------------------------------------------
# START SERVER
# --------------------------------------------------
async def main():
    async with websockets.serve(
        ws_handler,
        "0.0.0.0",
        PORT,
        process_request=process_request,
    ):
        await asyncio.Future()  # run forever


if __name__ == "__main__":
    asyncio.run(main())
