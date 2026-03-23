import asyncio
import json
import logging
import os
from typing import Dict, List, Optional, Set

import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

GATEWAY_HOST = os.getenv("GATEWAY_HOST", "0.0.0.0")
GATEWAY_PORT = int(os.getenv("GATEWAY_PORT", "8080"))
INITIAL_LEADER = os.getenv("INITIAL_LEADER", "http://replica1:5000")
CANDIDATES = os.getenv("CANDIDATES", "http://replica1:5000 http://replica2:5001 http://replica3:5002").split()
FRONTEND_DIR = os.getenv("FRONTEND_DIR", "/app/frontend")

app = FastAPI(title="Gateway")
clients: Set[WebSocket] = set()
leader_url: Optional[str] = INITIAL_LEADER

# FIX 1: Reduced timeout from 5.0 to 0.5 for immediate failover
client = httpx.AsyncClient(timeout=1.0)

async def pick_leader() -> Optional[str]:
    global leader_url
    for candidate in [leader_url] + CANDIDATES if leader_url else CANDIDATES:
        if not candidate:
            continue
        try:
            resp = await client.get(f"{candidate}/health")
            if resp.status_code == 200:
                data = resp.json()
                if data.get("state") == "leader":
                    leader_url = candidate
                    return leader_url

                leader_id = data.get("leader_id")
                if leader_id:
                    # Map the leader_id to a known candidate URL (includes correct port).
                    for c in CANDIDATES:
                        if leader_id in c:
                            leader_url = c
                            return leader_url
                    # Fallback: use leader_id as-is (may already be a full URL)
                    leader_url = leader_id
                    return leader_url
        except Exception:  # noqa: BLE001
            continue
    return None


async def fetch_committed_log() -> List[Dict]:
    url = await pick_leader()
    if not url:
        return []
    try:
        resp = await client.get(f"{url}/log")
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to fetch committed log: %s", exc)
        return []


async def submit_stroke(stroke: Dict) -> bool:
    global leader_url
    url = await pick_leader()
    if not url:
        return False
    try:
        resp = await client.post(f"{url}/submit-stroke", json={"stroke": stroke})
        resp.raise_for_status()
        data = resp.json()
        
        if data.get("accepted"):
            return True
            
        # FIX 2: Correctly map the leader hint if the stroke is rejected by a follower
        leader_hint = data.get("leader")
        if leader_hint:
            for c in CANDIDATES:
                if leader_hint in c:
                    leader_url = c
                    return False
            leader_url = leader_hint
        else:
            # Invalidate if rejected without a hint
            leader_url = None
            
        return False
        
    # FIX 3: Catch network connection drops specifically and immediately invalidate leader
    except httpx.RequestError as exc:
        logger.warning("submit_stroke failed (network error): %s", exc)
        leader_url = None  
        return False
    except Exception as exc:  # noqa: BLE001
        logger.warning("submit_stroke failed: %s", exc)
        return False


async def broadcast(message: Dict) -> None:
    dead: List[WebSocket] = []
    payload = json.dumps(message)
    for ws in clients:
        try:
            await ws.send_text(payload)
        except Exception:  # noqa: BLE001
            dead.append(ws)
    for ws in dead:
        clients.discard(ws)


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket) -> None:
    await ws.accept()
    clients.add(ws)
    history = await fetch_committed_log()
    await ws.send_text(json.dumps({"type": "history", "strokes": history}))
    try:
        while True:
            data = await ws.receive_json()
            if data.get("type") == "stroke":
                stroke = data.get("stroke", {})
                ok = await submit_stroke(stroke)
                if ok:
                    await broadcast({"type": "stroke", "stroke": stroke})
    except WebSocketDisconnect:
        clients.discard(ws)
    except Exception as exc:  # noqa: BLE001
        logger.warning("WebSocket error: %s", exc)
        clients.discard(ws)


@app.get("/")
async def root() -> HTMLResponse:
    try:
        with open(os.path.join(FRONTEND_DIR, "index.html"), "r", encoding="utf-8") as fp:
            html = fp.read()
    except FileNotFoundError:
        html = "<h1>Gateway running</h1>"
    return HTMLResponse(content=html)


@app.on_event("shutdown")
async def shutdown_event() -> None:
    await client.aclose()