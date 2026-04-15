from __future__ import annotations

import asyncio
import logging
import random
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import httpx

from .message_types import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    HealthResponse,
    LogEntryModel,
    SubmitStrokeRequest,
    SyncLogRequest,
    SyncLogResponse,
    VoteRequest,
    VoteResponse,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


@dataclass
class LogEntry:
    index: int
    term: int
    payload: Dict[str, Any]


class RaftNode:
    def __init__(
        self,
        node_id: str,
        host: str,
        port: int,
        peers: List[str],
        heartbeat_interval: float = 0.05,
        election_timeout_range: tuple[float, float] = (0.5, 2.0),
    ) -> None:
        self.id = node_id
        self.host = host
        self.port = port
        self.peers = peers

        self.state: str = "follower"
        self.current_term: int = 0
        self.voted_for: Optional[str] = None
        self.log: List[LogEntry] = []
        self.commit_index: int = 0
        self.last_applied: int = 0

        self.leader_id: Optional[str] = None
        self.next_index: Dict[str, int] = {}
        self.match_index: Dict[str, int] = {}

        self._election_timeout_range = election_timeout_range
        self._heartbeat_interval = heartbeat_interval
        self._last_heartbeat = time.monotonic()
        self._stop_event = asyncio.Event()
        self._tasks: List[asyncio.Task] = []
        self._client: Optional[httpx.AsyncClient] = None
        self._lock = asyncio.Lock()
        self._last_election_failed = 0.0

    async def start(self) -> None:
        self._client = httpx.AsyncClient(timeout=5.0)
        self._tasks.append(asyncio.create_task(self._run_election_timer()))
        logger.info("Node %s started with peers %s", self.id, self.peers)

    async def stop(self) -> None:
        self._stop_event.set()
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        if self._client:
            await self._client.aclose()

    # --------------------------- state transitions ---------------------------
    def _majority(self) -> int:
        return max(2, len(self.peers) // 2 + 1)

    def _last_log_index_term(self) -> tuple[int, int]:
        if not self.log:
            return 0, 0
        entry = self.log[-1]
        return entry.index, entry.term

    async def _run_election_timer(self) -> None:
        await asyncio.sleep(random.uniform(*self._election_timeout_range))
        current_timeout = random.uniform(*self._election_timeout_range)
        
        while not self._stop_event.is_set():
            await asyncio.sleep(0.05)
            if self.state == "leader":
                continue
            
            if time.monotonic() - self._last_election_failed < 4.0:
                continue
                 
            elapsed = time.monotonic() - self._last_heartbeat
            
            if elapsed >= current_timeout:
                await self._start_election()
                current_timeout = random.uniform(*self._election_timeout_range)

    async def _start_election(self) -> None:
        req_term = 0
        async with self._lock:
            self.state = "candidate"
            self.current_term += 1
            self.voted_for = self.id
            self.leader_id = None
            self._last_heartbeat = time.monotonic()  
            votes = 1
            req_term = self.current_term
            last_index, last_term = self._last_log_index_term()
            logger.info("%s starting election for term %s", self.id, req_term)

        responses = await asyncio.gather(
            *[self._send_vote_request(peer, req_term, last_index, last_term) for peer in self.peers],
            return_exceptions=True,
        )

        for res in responses:
            if isinstance(res, VoteResponse) and res.vote_granted and res.term >= req_term:
                votes += 1
            elif isinstance(res, VoteResponse) and res.term > req_term:
                async with self._lock:
                    self.current_term = res.term
                    self.state = "follower"
                    self.voted_for = None
                self._last_election_failed = time.monotonic()
                return

        if votes >= self._majority():
            await self._become_leader()
        else:
            async with self._lock:
                self.state = "follower"
                self.voted_for = None
            self._last_election_failed = time.monotonic()

    async def _become_leader(self) -> None:
        async with self._lock:
            self.state = "leader"
            self.leader_id = self.id
            last_index, _ = self._last_log_index_term()
            self.next_index = {peer: last_index + 1 for peer in self.peers}
            self.match_index = {peer: 0 for peer in self.peers}
            self._last_heartbeat = time.monotonic()
            self._last_election_failed = 0.0
        logger.info("%s became leader for term %s", self.id, self.current_term)
        self._tasks.append(asyncio.create_task(self._heartbeat_loop()))

    async def _heartbeat_loop(self) -> None:
        while not self._stop_event.is_set() and self.state == "leader":
            await self.replicate()
            await asyncio.sleep(self._heartbeat_interval)

    # ------------------------------- RPC senders ------------------------------
    async def _send_vote_request(self, peer: str, term: int, last_index: int, last_term: int) -> Optional[VoteResponse]:
        if not self._client:
            return None
        try:
            resp = await self._client.post(
                f"{peer}/request-vote",
                json=VoteRequest(
                    term=term,
                    candidate_id=self.id,
                    last_log_index=last_index,
                    last_log_term=last_term,
                ).model_dump(),
            )
            resp.raise_for_status()
            return VoteResponse(**resp.json())
        except Exception as exc:  # noqa: BLE001
            return None

    async def replicate(self) -> None:
        if self.state != "leader" or not self._client:
            return
            
        tasks = []
        has_new_entries = False
        
        for peer in self.peers:
            prev_index = self.next_index.get(peer, 1) - 1
            prev_term = self.log[prev_index - 1].term if prev_index > 0 and prev_index <= len(self.log) else 0
            
            entries_to_send = self.log[prev_index:]
            if entries_to_send:
                has_new_entries = True
                
            payload_entries = [LogEntryModel(index=e.index, term=e.term, payload=e.payload).model_dump() for e in entries_to_send]
            req = AppendEntriesRequest(
                term=self.current_term,
                leader_id=self.id,
                prev_log_index=prev_index,
                prev_log_term=prev_term,
                entries=payload_entries,
                leader_commit=self.commit_index,
            )
            tasks.append(self._send_append_entries(peer, req))
            
        results = await asyncio.gather(*tasks, return_exceptions=True)
        await self._handle_replication_results(results, has_new_entries)

    async def _send_append_entries(self, peer: str, req: AppendEntriesRequest) -> Optional[AppendEntriesResponse]:
        try:
            resp = await self._client.post(f"{peer}/append-entries", json=req.model_dump())
            resp.raise_for_status()
            return AppendEntriesResponse(**resp.json())
        except Exception as exc:  # noqa: BLE001
            return None

    async def _send_sync_log(self, peer: str, from_index: int) -> Optional[AppendEntriesResponse]:
        if not self._client:
            return None
        try:
            slice_entries = [e for e in self.log if e.index >= from_index]
            payload_entries = [LogEntryModel(index=e.index, term=e.term, payload=e.payload).model_dump() for e in slice_entries]
            req = SyncLogResponse(term=self.current_term, entries=payload_entries, commit_index=self.commit_index)
            resp = await self._client.post(f"{peer}/sync-log", json=req.model_dump())
            resp.raise_for_status()
            return AppendEntriesResponse(**resp.json())
        except Exception as exc:  # noqa: BLE001
            return None

    async def _handle_replication_results(self, results: List[Any], has_new_entries: bool) -> None:
        for peer, res in zip(self.peers, results, strict=False):
            if not isinstance(res, AppendEntriesResponse):
                continue
            if res.term > self.current_term:
                async with self._lock:
                    self.current_term = res.term
                    self.state = "follower"
                    self.voted_for = None
                return
            if res.success:
                self.match_index[peer] = res.match_index
                self.next_index[peer] = res.match_index + 1
            else:
                await self._send_sync_log(peer, res.follower_len)
        if has_new_entries:
            await self._advance_commit_index()

    async def _advance_commit_index(self) -> None:
        match_indexes = list(self.match_index.values()) + [len(self.log)]
        match_indexes.sort()
        quorum_index = match_indexes[len(match_indexes) // 2]
        if quorum_index > self.commit_index and self.log[quorum_index - 1].term == self.current_term:
            self.commit_index = quorum_index
            logger.info("%s advanced commit_index to %s", self.id, self.commit_index)

    # ------------------------------- RPC handlers -----------------------------
    async def handle_request_vote(self, req: VoteRequest) -> VoteResponse:
        async with self._lock:
            if req.term < self.current_term:
                return VoteResponse(term=self.current_term, vote_granted=False, voter_id=self.id)
            if req.term > self.current_term:
                self.current_term = req.term
                self.voted_for = None
                self.state = "follower"
            up_to_date = (req.last_log_term, req.last_log_index) >= self._last_log_index_term()
            can_vote = self.voted_for in (None, req.candidate_id)
            if can_vote and up_to_date:
                self.voted_for = req.candidate_id
                self._last_heartbeat = time.monotonic()
                self._last_election_failed = time.monotonic() + 10.0
                return VoteResponse(term=self.current_term, vote_granted=True, voter_id=self.id)
            return VoteResponse(term=self.current_term, vote_granted=False, voter_id=self.id)

    async def handle_append_entries(self, req: AppendEntriesRequest) -> AppendEntriesResponse:
        async with self._lock:
            if req.term < self.current_term:
                return AppendEntriesResponse(term=self.current_term, success=False, match_index=len(self.log), follower_id=self.id, follower_len=len(self.log))
            
            self._last_heartbeat = time.monotonic()
            
            if req.term > self.current_term:
                self.current_term = req.term
                self.state = "follower"
                self.voted_for = None
            
            if req.term == self.current_term and self.state == "leader" and req.leader_id != self.id:
                self.state = "follower"
                self.leader_id = req.leader_id
                
            self.leader_id = req.leader_id
            
            if req.prev_log_index > 0:
                if req.prev_log_index > len(self.log) or self.log[req.prev_log_index - 1].term != req.prev_log_term:
                    return AppendEntriesResponse(term=self.current_term, success=False, match_index=len(self.log), follower_id=self.id, follower_len=len(self.log))
            
            new_entries = [LogEntry(index=e.index, term=e.term, payload=e.payload) for e in req.entries]
            for entry in new_entries:
                local_index = entry.index - 1
                if local_index < len(self.log):
                    if self.log[local_index].term != entry.term:
                        self.log = self.log[:local_index]
                        self.log.append(entry)
                else:
                    self.log.append(entry)
                    
            if req.leader_commit > self.commit_index:
                self.commit_index = min(req.leader_commit, len(self.log))
                
            return AppendEntriesResponse(term=self.current_term, success=True, match_index=len(self.log), follower_id=self.id, follower_len=len(self.log))

    async def handle_sync_log(self, req: SyncLogResponse) -> AppendEntriesResponse:
        async with self._lock:
            self.current_term = max(self.current_term, req.term)
            
            if req.entries:
                first_sync_index = req.entries[0].index
                self.log = self.log[:first_sync_index - 1] 
                
                for e in req.entries:
                    self.log.append(LogEntry(index=e.index, term=e.term, payload=e.payload))
                    
            self.commit_index = max(self.commit_index, req.commit_index)
            return AppendEntriesResponse(term=self.current_term, success=True, match_index=len(self.log), follower_id=self.id, follower_len=len(self.log))

    async def handle_submit_stroke(self, req: SubmitStrokeRequest) -> Dict[str, Any]:
        if self.state != "leader":
            return {"accepted": False, "leader": self.leader_id}
            
        async with self._lock:
            new_index = len(self.log) + 1
            entry = LogEntry(index=new_index, term=self.current_term, payload=req.stroke)
            self.log.append(entry)
            
        asyncio.create_task(self.replicate())
        return {"accepted": True, "index": new_index}

    async def snapshot(self) -> HealthResponse:
        return HealthResponse(
            id=self.id,
            term=self.current_term,
            state=self.state,
            leader_id=self.leader_id,
            log_length=len(self.log),
            commit_index=self.commit_index,
        )

    async def committed_log(self) -> List[Dict[str, Any]]:
        return [entry.payload for entry in self.log[: self.commit_index]]

    async def redirect_leader(self) -> Optional[str]:
        return self.leader_id