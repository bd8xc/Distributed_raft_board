import os
import asyncio
import logging
from typing import List

from fastapi import FastAPI
from fastapi.responses import PlainTextResponse

from shared.message_types import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    HealthResponse,
    SubmitStrokeRequest,
    SyncLogResponse,
    VoteRequest,
    VoteResponse,
)
from shared.raft_node import RaftNode

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

app = FastAPI(title="Mini RAFT Replica")

REPLICA_ID = os.getenv("REPLICA_ID", "replica3")
RPC_HOST = os.getenv("RPC_HOST", "0.0.0.0")
RPC_PORT = int(os.getenv("RPC_PORT", "5002"))
PEERS = os.getenv("PEERS", "").split()

node = RaftNode(
    node_id=REPLICA_ID,
    host=RPC_HOST,
    port=RPC_PORT,
    peers=PEERS,
)


@app.on_event("startup")
async def startup_event() -> None:
    asyncio.create_task(node.start())


@app.on_event("shutdown")
async def shutdown_event() -> None:
    await node.stop()


@app.post("/request-vote", response_model=VoteResponse)
async def request_vote(req: VoteRequest) -> VoteResponse:
    return await node.handle_request_vote(req)


@app.post("/append-entries", response_model=AppendEntriesResponse)
async def append_entries(req: AppendEntriesRequest) -> AppendEntriesResponse:
    return await node.handle_append_entries(req)


@app.post("/heartbeat", response_model=AppendEntriesResponse)
async def heartbeat(req: AppendEntriesRequest) -> AppendEntriesResponse:
    # Heartbeat uses the same AppendEntries RPC with no new entries.
    return await node.handle_append_entries(req)


@app.post("/sync-log", response_model=AppendEntriesResponse)
async def sync_log(req: SyncLogResponse) -> AppendEntriesResponse:
    return await node.handle_sync_log(req)


@app.post("/submit-stroke")
async def submit_stroke(req: SubmitStrokeRequest) -> dict:
    return await node.handle_submit_stroke(req)


@app.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    return await node.snapshot()


@app.get("/log", response_model=List[dict])
async def committed_log() -> List[dict]:
    return await node.committed_log()

@app.get("/ping")
async def ping() -> dict:
    return {"status": "alive", "id": REPLICA_ID, "term": node.current_term}

@app.get("/metrics")
async def metrics() -> PlainTextResponse:
    # 1 if leader, 0 otherwise
    is_leader = 1 if node.state == "leader" else 0
    
    # Prometheus plain-text format
    metrics_data = (
        f'raft_term{{node="{REPLICA_ID}"}} {node.current_term}\n'
        f'raft_log_length{{node="{REPLICA_ID}"}} {len(node.log)}\n'
        f'raft_is_leader{{node="{REPLICA_ID}"}} {is_leader}\n'
    )
    return PlainTextResponse(content=metrics_data)

@app.get("/")
async def root() -> dict:
    return {"id": REPLICA_ID, "term": node.current_term, "state": node.state}
