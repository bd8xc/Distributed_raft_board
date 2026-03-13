# Distributed RAFT Drawing Board (Python)

This is a minimal end-to-end scaffold for the assignment: gateway WebSocket fanout, three RAFT-lite replicas, and a static canvas frontend. Everything runs via `docker-compose` with bind-mounted code for hot reload.

## Layout
- gateway/ — FastAPI WebSocket gateway that routes strokes to the current leader and broadcasts commits.
- replica1, replica2, replica3/ — identical FastAPI RAFT nodes; identity set by env vars.
- shared/ — common RAFT logic and message schemas.
- replica/Dockerfile — build recipe for all replicas.
- gateway/Dockerfile — build recipe for gateway.
- frontend/ — static canvas client served by the gateway.

## Quick start
```sh
# from repo root
docker-compose up --build
```
Then open http://localhost:8080 (gateway serves the frontend). Draw in multiple tabs to see live sync.

## How it works (simplified)
- Each replica runs `RaftNode` with follower/candidate/leader states, randomized election timeout (500–800ms), and 150ms heartbeats.
- Gateway keeps a list of candidate replica URLs and probes `/health` to find the leader. Strokes sent via WebSocket are forwarded to `/submit-stroke` on the leader; once accepted, the stroke is broadcast to all clients.
- Replication: leader appends strokes to its log, sends `AppendEntries` to followers, advances commit on quorum, and followers expose `/sync-log` for catch-up.
- Logs are in-memory for clarity; bind mounts plus `--reload` enable hot code reload without container restarts.

## Key endpoints (replicas)
- POST /request-vote — election
- POST /append-entries — replication + heartbeat
- POST /sync-log — leader pushes missing entries to a follower
- POST /submit-stroke — client-facing write (leader only)
- GET /log — committed strokes
- GET /health — state snapshot

## Notes / next steps
- Persistence: logs are memory-only; swap to a file-backed store if you need durability across container restarts.
- Robust leader discovery: gateway currently probes candidates; adding leader redirect hints or a heartbeat subscription would improve failover speed.
- Tests: add integration tests that kill/respawn replicas and assert canvas consistency.
