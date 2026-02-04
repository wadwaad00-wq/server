#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# server_ws.py — WebSocket hopper backend (PROVEN - MINIMUM 5 PLAYERS)
# Python 3.10+ ;  pip install websockets aiohttp

import asyncio, json, time
from urllib.parse import urlparse, parse_qs
import aiohttp
import websockets

TTL_LOCK     = 60_000          # 60s soft lock during teleport
TTL_USED     = 60 * 60 * 1000  # 1h hard block (no duplicates)
MIN_FREE_SLOTS = 1             # Minimum 1 free slot (to join)
PAGE_LIMIT   = 100             # Roblox API limit
QUEUE_TARGET = 600             # Large queue to prevent bottlenecks
CLEAN_EVERY  = 2.0             # seconds
STALL_MS     = 120_000

ORDERS = ("Asc", "Desc")

def now_ms(): return int(time.time() * 1000)

class Universe:
    __slots__ = ("uid","min_players","queue","locks","used","seen",
                 "cursor","delay","last_req","scanning","waiters","last_progress","pop_lock","server_timestamps")
    def __init__(self, uid: str):
        self.uid = uid
        self.min_players = 5  # DEFAULT: 5 players (can override via query param)
        self.queue = []
        self.locks = {}      # id -> ts
        self.used  = {}      # id -> ts
        self.seen  = set()
        self.cursor = {"Asc": None, "Desc": None}
        self.delay  = {"Asc": 800,  "Desc": 800}
        self.last_req = {"Asc": 0, "Desc": 0}
        self.scanning = False
        self.waiters = []    # list[(future,current_job,who)]
        self.last_progress = now_ms()
        self.pop_lock = asyncio.Lock()  # ATOMIC LOCK!
        self.server_timestamps = {}  # id -> timestamp when added

universes = {}

def getU(uid: str) -> Universe:
    uid = str(uid)
    st = universes.get(uid)
    if not st:
        st = Universe(uid)
        universes[uid] = st
        asyncio.create_task(scanner_loop(st))
    return st

def is_joinable_base(s):
    playing = int(s.get("playing") or s.get("playerCount") or s.get("currentPlayers") or 0)
    maxp    = int(s.get("maxPlayers") or s.get("maxPlayerCount") or s.get("max") or 0)
    if not maxp or playing >= maxp: return False
    if s.get("vipServerId") or s.get("privateServerId") or s.get("accessCode"): return False
    t = str(s.get("serverType","public")).lower()
    return t == "public"

def is_joinable_with_min(s, min_players:int):
    """
    Checks if server has:
    1. Minimum X players ALREADY PLAYING (min_players)
    2. At least 1 free slot (to join)
    """
    if not is_joinable_base(s): 
        return False
    
    playing = int(s.get("playing") or s.get("playerCount") or s.get("currentPlayers") or 0)
    maxp = int(s.get("maxPlayers") or s.get("maxPlayerCount") or s.get("max") or 0)
    
    # 1. CRITICAL: Check minimum playing players
    if min_players > 0:
        if playing < min_players: 
            return False
    
    # 2. Check free slots (to actually join)
    if maxp > 0:
        free_slots = maxp - playing
        if free_slots < MIN_FREE_SLOTS:
            return False
    
    return True

async def fetch_page(session:aiohttp.ClientSession, uid:str, cursor:str|None, order:str):
    base = f"https://games.roblox.com/v1/games/{uid}/servers/Public?sortOrder={order}&limit={PAGE_LIMIT}&excludeFullGames=true"
    url = f"{base}&cursor={cursor}" if cursor else base
    async with session.get(url, headers={"Accept":"application/json"}) as r:
        if r.status == 429: raise RuntimeError("RATE_429")
        r.raise_for_status()
        return await r.json()

def push_servers(st:Universe, arr):
    pushed = 0
    skipped_low_players = 0
    skipped_full = 0
    
    for sv in arr or []:
        playing = int(sv.get("playing") or sv.get("playerCount") or sv.get("currentPlayers") or 0)
        maxp = int(sv.get("maxPlayers") or sv.get("maxPlayerCount") or sv.get("max") or 0)
        
        if playing < st.min_players:
            skipped_low_players += 1
            continue
            
        if maxp > 0 and (maxp - playing) < MIN_FREE_SLOTS:
            skipped_full += 1
            continue
        
        if not is_joinable_with_min(sv, st.min_players): 
            continue
            
        sid = str(sv.get("id") or sv.get("jobId") or "")
        if not sid: 
            continue
        if sid in st.seen or sid in st.locks or sid in st.used: 
            continue
        
        st.queue.append(sid)
        st.seen.add(sid)
        pushed += 1
    
    if pushed > 0:
        print(f"[SCAN] +{pushed} servers | Queue: {len(st.queue)} | Skipped: low={skipped_low_players} full={skipped_full}")
    
    return pushed

def try_pop_available(st:Universe, current_job:str|None, who:str|None):
    """
    CRITICAL: Thread-safe server popping with STRICT duplicate prevention
    """
    while st.queue:
        sid = st.queue.pop(0)
        
        # Skip if this is client's current server
        if current_job and str(sid) == str(current_job):
            continue
        
        # CRITICAL: Double-check locks/used BEFORE marking
        if sid in st.locks or sid in st.used:
            continue
        
        # INSTANT HARD LOCK - mark as used IMMEDIATELY (before returning!)
        ts = now_ms()
        st.locks[sid] = ts
        st.used[sid]  = ts
        
        print(f"[SEND] Server {sid} → {who} (min={st.min_players}) ✅ LOCKED")
        return sid
    
    return None

def drain_waiters(st:Universe):
    """Drain waiters with atomic lock protection"""
    if not st.waiters or not st.queue:
        return
    remaining = []
    for fut, current_job, who in st.waiters:
        if fut.done():
            continue
        
        # Try to pop server (already atomic in try_pop_available)
        sid = try_pop_available(st, current_job, who)
        if sid:
            fut.set_result(sid)
        else:
            remaining.append((fut,current_job,who))
    st.waiters = remaining

async def scan_tick(st:Universe, order:str, session:aiohttp.ClientSession):
    now = now_ms()
    dt  = now - st.last_req[order]
    if dt < st.delay[order]:
        await asyncio.sleep((st.delay[order] - dt)/1000)
    st.last_req[order] = now_ms()

    # FRESH SCAN: Reset cursor co 30s żeby dostać świeże serwery!
    if now - st.last_progress > 30_000:  # 30s
        st.cursor["Asc"] = None
        st.cursor["Desc"]= None
        st.delay["Asc"]  = max(400, int(st.delay["Asc"]  * 0.8))
        st.delay["Desc"] = max(400, int(st.delay["Desc"] * 0.8))
        st.last_progress = now_ms()
        print(f"[REFRESH] Resetting cursors for fresh servers!")

    try:
        page = await fetch_page(session, st.uid, st.cursor[order], order)
        st.cursor[order] = page.get("nextPageCursor")
        pushed = push_servers(st, page.get("data") or [])
        if pushed:
            st.last_progress = now_ms()
            st.delay[order] = max(400, int(st.delay[order] * 0.85))
            drain_waiters(st)
        else:
            await asyncio.sleep(0.1)
    except Exception as e:
        st.delay[order] = min(4000, int(st.delay[order] * 1.5))
        await asyncio.sleep(st.delay[order]/1000)

async def scanner_loop(st:Universe):
    if st.scanning: 
        return
    st.scanning = True
    async with aiohttp.ClientSession() as session:
        while True:
            if len(st.queue) < QUEUE_TARGET or st.waiters:
                await asyncio.gather(*(scan_tick(st, o, session) for o in ORDERS))
            else:
                await scan_tick(st, "Asc", session)
            if len(st.seen) > 80_000:
                keep = set(st.queue)
                st.seen = keep.copy()

async def cleaner():
    while True:
        await asyncio.sleep(CLEAN_EVERY)
        now = now_ms()
        for st in universes.values():
            for sid, ts in list(st.locks.items()):
                if now - ts > TTL_LOCK:
                    st.locks.pop(sid, None)
            for sid, ts in list(st.used.items()):
                if now - ts > TTL_USED:
                    st.used.pop(sid, None)
            if st.queue and st.waiters:
                drain_waiters(st)

async def ws_send_next(st:Universe, ws, current_job:str|None, who:str|None):
    """
    CRITICAL: Send next server with ATOMIC lock to prevent duplicates
    """
    # ATOMIC: Lock during pop to prevent race conditions
    async with st.pop_lock:
        sid = try_pop_available(st, current_job, who)
    
    if sid:
        await ws.send(json.dumps({"type":"next", "id": sid}))
        return
    
    # Wait like long-poll
    fut = asyncio.get_event_loop().create_future()
    st.waiters.append((fut, current_job, who))
    try:
        sid = await asyncio.wait_for(fut, timeout=10.0)
        if sid:
            await ws.send(json.dumps({"type":"next","id":sid}))
    except asyncio.TimeoutError:
        # No response (client will request again)
        pass

async def handle_ws(ws):
    # Compatibility with websockets>=12
    path = "/ws"
    if hasattr(ws, "request") and getattr(ws.request, "path", None):
        path = ws.request.path
    try:
        p = urlparse(path)
        q = {k:v[0] for k,v in parse_qs(p.query).items()}
    except Exception:
        q = {}

    uid = q.get("placeId") or q.get("universeId") or q.get("uid")
    who = q.get("who") or "?"
    min_players = int(q.get("minPlayers") or 5)  # DEFAULT 5!

    if not uid:
        await ws.send(json.dumps({"type":"error","error":"no placeId"}))
        await ws.close()
        return

    st = getU(uid)
    st.min_players = min_players

    print(f"[WS] +conn uid={uid} who={who} minPlayers={min_players}")

    current_job = None

    # Send next immediately on start
    await ws_send_next(st, ws, current_job, who)

    try:
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except Exception:
                continue

            t = str(msg.get("type") or "")
            if t == "hello":
                current_job = str(msg.get("currentJob") or "") or None
                await ws_send_next(st, ws, current_job, who)
            elif t == "next":
                current_job = str(msg.get("currentJob") or "") or None
                await ws_send_next(st, ws, current_job, who)
            elif t == "release":
                sid = str(msg.get("id") or msg.get("serverId") or "")
                if sid:
                    st.locks.pop(sid, None)  # used remains (1h)
            elif t == "joined":
                pass  # Just log
    finally:
        print(f"[WS] -conn uid={uid} who={who}")

async def main():
    print("="*60)
    print("🚀 HOPPER BACKEND - WebSocket Edition")
    print("="*60)
    print(f"📡 Port: 3001")
    print(f"🔒 Server Lock: 1 hour (no duplicates)")
    print(f"👥 Default Min Players: 5 (already playing)")
    print(f"📊 Min Free Slots: {MIN_FREE_SLOTS} (to join)")
    print(f"⏱️  Soft Lock: {TTL_LOCK/1000}s (during teleport)")
    print(f"📦 Queue Target: {QUEUE_TARGET} servers")
    print(f"🎯 Strategy: Dual-direction + Atomic locks")
    print("="*60)
    
    asyncio.create_task(cleaner())
    async with websockets.serve(handle_ws, "0.0.0.0", 3001, max_size=1<<20):
        await asyncio.Future()  # run forever

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Shutdown")
        pass
