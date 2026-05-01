import asyncio
import random
import time
import logging
from enum import Enum
from dataclasses import dataclass, field, asdict
from typing import Optional, List, Dict, Any, Callable

from src.utils.config import config
from src.utils.metrics import metrics

logger = logging.getLogger(__name__)


class NodeRole(Enum):
    FOLLOWER  = "follower"
    CANDIDATE = "candidate"
    LEADER    = "leader"


@dataclass
class LogEntry:
    term: int
    index: int
    command: Dict
    committed: bool = False

    def to_dict(self) -> Dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict) -> "LogEntry":
        return cls(**d)


@dataclass
class VoteRequest:
    term: int
    candidate_id: int
    last_log_index: int
    last_log_term: int


@dataclass
class VoteResponse:
    term: int
    vote_granted: bool


@dataclass
class AppendEntriesRequest:
    term: int
    leader_id: int
    prev_log_index: int
    prev_log_term: int
    entries: List[Dict]
    leader_commit: int


@dataclass
class AppendEntriesResponse:
    term: int
    success: bool
    match_index: int = 0


class RaftNode:
    def __init__(self, node_id: int, peers: List[int]):
        self.node_id = node_id
        self.peers = peers

        self.current_term = 0
        self.voted_for: Optional[int] = None
        self.log: List[LogEntry] = []

        self.role = NodeRole.FOLLOWER
        self.commit_index = 0
        self.last_applied = 0
        self.current_leader: Optional[int] = None

        self.next_index: Dict[int, int] = {}
        self.match_index: Dict[int, int] = {}

        self._election_timeout = self._new_election_timeout()
        self._last_heartbeat = time.time()

        # PERBAIKAN: list callbacks, bukan satu callback
        self._state_machine_callbacks: List[Callable] = []

        self._tasks: List[asyncio.Task] = []
        self._running = False
        self.rpc_sender = None

        logger.info(f"RaftNode {self.node_id} initialized, peers={self.peers}")

    async def start(self):
        """Mulai election timer dan apply loop."""
        self._running = True
        metrics.set_role("follower")
        self._tasks = [
            asyncio.create_task(self._election_timer_loop()),
            asyncio.create_task(self._apply_committed_entries_loop()),
        ]
        logger.info(f"Node {self.node_id} started as FOLLOWER")

    async def stop(self):
        """Hentikan semua background tasks."""
        self._running = False
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

    def register_state_machine(self, callback: Callable):
        """Daftarkan callback — semua callback dipanggil saat ada entry committed."""
        self._state_machine_callbacks.append(callback)

    async def submit_command(self, command: Dict) -> Optional[int]:
        """Terima command dari client, masukkan ke log, replikasi. Hanya di leader."""
        if self.role != NodeRole.LEADER:
            return None
        new_index = len(self.log) + 1
        entry = LogEntry(term=self.current_term, index=new_index, command=command)
        self.log.append(entry)
        logger.info(f"Leader {self.node_id} appended entry index={new_index}: {command}")
        await self._replicate_to_all()
        return new_index

    async def wait_for_commit(self, index: int, timeout: float = 5.0) -> bool:
        """Tunggu sampai log entry pada index committed oleh mayoritas."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.commit_index >= index:
                return True
            await asyncio.sleep(0.01)
        return False

    def _new_election_timeout(self) -> float:
        ms = random.randint(config.raft.election_timeout_min, config.raft.election_timeout_max)
        return ms / 1000.0

    async def _election_timer_loop(self):
        """Pantau heartbeat; mulai election jika timeout."""
        while self._running:
            await asyncio.sleep(0.01)
            if self.role == NodeRole.LEADER:
                await self._send_heartbeats()
                await asyncio.sleep(config.raft.heartbeat_interval / 1000.0)
                continue
            elapsed = time.time() - self._last_heartbeat
            if elapsed >= self._election_timeout:
                await self._start_election()

    async def _start_election(self):
        """Naikkan term, vote sendiri, minta suara peer."""
        self.role = NodeRole.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self._last_heartbeat = time.time()
        self._election_timeout = self._new_election_timeout()
        metrics.set_role("candidate")
        metrics.election_total.labels(node_id=str(self.node_id)).inc()

        votes_received = 1
        votes_needed = (len(self.peers) + 1) // 2 + 1
        last_log_index = len(self.log)
        last_log_term = self.log[-1].term if self.log else 0

        vote_request = VoteRequest(
            term=self.current_term, candidate_id=self.node_id,
            last_log_index=last_log_index, last_log_term=last_log_term
        )
        responses = await asyncio.gather(
            *[self._request_vote_from(pid, vote_request) for pid in self.peers],
            return_exceptions=True
        )
        for response in responses:
            if isinstance(response, Exception) or not isinstance(response, VoteResponse):
                continue
            if response.term > self.current_term:
                await self._become_follower(response.term)
                return
            if response.vote_granted:
                votes_received += 1

        if self.role == NodeRole.CANDIDATE and votes_received >= votes_needed:
            await self._become_leader()
        else:
            self.role = NodeRole.FOLLOWER
            metrics.set_role("follower")

    async def _request_vote_from(self, peer_id: int, request: VoteRequest) -> Optional[VoteResponse]:
        """Kirim RequestVote ke satu peer."""
        try:
            if self.rpc_sender is None:
                return None
            data = await self.rpc_sender.send_vote_request(peer_id, request)
            if data:
                return VoteResponse(**data)
        except Exception as e:
            logger.debug(f"RequestVote ke peer {peer_id} gagal: {e}")
        return None

    async def _become_leader(self):
        """Inisialisasi state leader dan kirim heartbeat."""
        self.role = NodeRole.LEADER
        self.current_leader = self.node_id
        metrics.set_role("leader")
        for peer_id in self.peers:
            self.next_index[peer_id] = len(self.log) + 1
            self.match_index[peer_id] = 0
        logger.info(f"*** Node {self.node_id} jadi LEADER di term {self.current_term} ***")
        await self._send_heartbeats()

    async def _become_follower(self, term: int):
        """Mundur jadi follower karena term lebih tinggi."""
        self.current_term = term
        self.role = NodeRole.FOLLOWER
        self.voted_for = None
        self._last_heartbeat = time.time()
        metrics.set_role("follower")

    async def handle_vote_request(self, request: VoteRequest) -> VoteResponse:
        """Proses RequestVote masuk dari candidate."""
        if request.term > self.current_term:
            await self._become_follower(request.term)
        if request.term < self.current_term:
            return VoteResponse(term=self.current_term, vote_granted=False)

        already_voted = self.voted_for is not None and self.voted_for != request.candidate_id
        if already_voted:
            return VoteResponse(term=self.current_term, vote_granted=False)

        my_last_log_index = len(self.log)
        my_last_log_term = self.log[-1].term if self.log else 0
        candidate_log_ok = (
            request.last_log_term > my_last_log_term or
            (request.last_log_term == my_last_log_term and request.last_log_index >= my_last_log_index)
        )
        if not candidate_log_ok:
            return VoteResponse(term=self.current_term, vote_granted=False)

        self.voted_for = request.candidate_id
        self._last_heartbeat = time.time()
        return VoteResponse(term=self.current_term, vote_granted=True)

    async def handle_append_entries(self, request: AppendEntriesRequest) -> AppendEntriesResponse:
        """Proses AppendEntries dari leader."""
        if request.term < self.current_term:
            return AppendEntriesResponse(term=self.current_term, success=False)
        if request.term > self.current_term:
            self.current_term = request.term
            self.voted_for = None

        self._last_heartbeat = time.time()
        self.role = NodeRole.FOLLOWER
        self.current_leader = request.leader_id
        metrics.set_role("follower")

        if request.prev_log_index > 0:
            if len(self.log) < request.prev_log_index:
                return AppendEntriesResponse(term=self.current_term, success=False)
            existing_entry = self.log[request.prev_log_index - 1]
            if existing_entry.term != request.prev_log_term:
                self.log = self.log[:request.prev_log_index - 1]
                return AppendEntriesResponse(term=self.current_term, success=False)

        for entry_dict in request.entries:
            entry = LogEntry.from_dict(entry_dict)
            if entry.index <= len(self.log):
                if self.log[entry.index - 1].term != entry.term:
                    self.log = self.log[:entry.index - 1]
                    self.log.append(entry)
            else:
                self.log.append(entry)

        if request.leader_commit > self.commit_index:
            self.commit_index = min(request.leader_commit, len(self.log))

        return AppendEntriesResponse(term=self.current_term, success=True, match_index=len(self.log))

    async def _send_heartbeats(self):
        """Kirim heartbeat ke semua follower."""
        if self.role != NodeRole.LEADER:
            return
        await asyncio.gather(*[self._send_append_entries(pid) for pid in self.peers], return_exceptions=True)
        metrics.heartbeat_sent.labels(node_id=str(self.node_id)).inc()

    async def _replicate_to_all(self):
        """Replikasi log ke semua follower dan update commit index."""
        if self.role != NodeRole.LEADER:
            return
        responses = await asyncio.gather(
            *[self._send_append_entries(pid) for pid in self.peers],
            return_exceptions=True
        )
        success_count = 1
        for resp in responses:
            if isinstance(resp, AppendEntriesResponse) and resp.success:
                success_count += 1
        quorum = (len(self.peers) + 1) // 2 + 1
        if success_count >= quorum:
            new_commit = len(self.log)
            if new_commit > self.commit_index:
                self.commit_index = new_commit
                logger.info(f"Leader {self.node_id} commit index naik ke {self.commit_index}")

    async def _send_append_entries(self, peer_id: int) -> Optional[AppendEntriesResponse]:
        """Kirim AppendEntries ke satu peer."""
        if self.role != NodeRole.LEADER:
            return None
        next_idx = self.next_index.get(peer_id, 1)
        prev_log_index = next_idx - 1
        prev_log_term = self.log[prev_log_index - 1].term if prev_log_index > 0 and prev_log_index <= len(self.log) else 0
        entries_to_send = [e.to_dict() for e in self.log[next_idx - 1:]] if next_idx <= len(self.log) else []

        request = AppendEntriesRequest(
            term=self.current_term, leader_id=self.node_id,
            prev_log_index=prev_log_index, prev_log_term=prev_log_term,
            entries=entries_to_send, leader_commit=self.commit_index
        )
        try:
            if self.rpc_sender is None:
                return None
            data = await self.rpc_sender.send_append_entries(peer_id, request)
            if not data:
                return None
            response = AppendEntriesResponse(**data)
            if response.term > self.current_term:
                await self._become_follower(response.term)
                return response
            if response.success:
                self.match_index[peer_id] = response.match_index
                self.next_index[peer_id] = response.match_index + 1
            else:
                self.next_index[peer_id] = max(1, self.next_index[peer_id] - 1)
            return response
        except Exception as e:
            logger.debug(f"AppendEntries ke peer {peer_id} gagal: {e}")
            return None

    async def _apply_committed_entries_loop(self):
        """Eksekusi log entries committed ke SEMUA state machine callbacks."""
        while self._running:
            await asyncio.sleep(0.005)
            while self.last_applied < self.commit_index:
                self.last_applied += 1
                entry = self.log[self.last_applied - 1]
                entry.committed = True
                logger.debug(f"Node {self.node_id} apply entry index={entry.index}: {entry.command}")

                # PERBAIKAN: panggil SEMUA callback, bukan hanya satu
                for callback in self._state_machine_callbacks:
                    try:
                        await callback(entry.command)
                    except Exception as e:
                        logger.error(f"State machine error pada entry {entry.index}: {e}")

    def get_status(self) -> Dict:
        """Return status node untuk health check."""
        return {
            "node_id": self.node_id,
            "role": self.role.value,
            "current_term": self.current_term,
            "current_leader": self.current_leader,
            "log_length": len(self.log),
            "commit_index": self.commit_index,
            "last_applied": self.last_applied,
            "peers": self.peers,
        }

    def is_leader(self) -> bool:
        return self.role == NodeRole.LEADER

    def get_leader_id(self) -> Optional[int]:
        return self.current_leader