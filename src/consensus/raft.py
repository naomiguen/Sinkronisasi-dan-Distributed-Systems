import asyncio
import random
import time
import json
import logging
from enum import Enum
from dataclasses import dataclass, field, asdict
from typing import Optional, List, Dict, Any, Callable

from src.utils.config import config
from src.utils.metrics import metrics

logger = logging.getLogger(__name__)


# ENUMS & DATA CLASSES

class NodeRole(Enum):
    """Peran node dalam cluster Raft."""
    FOLLOWER  = "follower"   # menerima perintah dari leader
    CANDIDATE = "candidate"  # sedang meminta suara
    LEADER    = "leader"     # memimpin cluster


@dataclass
class LogEntry:
    #Satu entry dalam Raft log. Dianggap "committed" setelah mayoritas node menyimpannya.
    term: int           # term saat entry dibuat
    index: int          # posisi dalam log (mulai dari 1)
    command: Dict       # perintah yang akan dieksekusi, contoh: {"op": "LOCK", "key": "db"}
    committed: bool = False

    def to_dict(self) -> Dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict) -> "LogEntry":
        return cls(**d)


@dataclass
class VoteRequest:
    """Pesan candidate ke semua node saat election."""
    term: int
    candidate_id: int
    last_log_index: int
    last_log_term: int


@dataclass
class VoteResponse:
    """Balasan node yang diminta suara."""
    term: int
    vote_granted: bool


@dataclass
class AppendEntriesRequest:
    #Pesan dari leader ke follower. Dipakai untuk replikasi log dan heartbeat (entries kosong).
    term: int
    leader_id: int
    prev_log_index: int
    prev_log_term: int
    entries: List[Dict]
    leader_commit: int


@dataclass
class AppendEntriesResponse:
    """Balasan follower ke leader."""
    term: int
    success: bool
    match_index: int = 0


# RAFT NODE
class RaftNode:
    """
    Implementasi Raft Consensus untuk satu node.
    Berkomunikasi dengan node lain via RPC di communication/message_passing.py.

    Cara pakai:
        node = RaftNode(node_id=1, peers=[2, 3])
        await node.start()
    """

    def __init__(self, node_id: int, peers: List[int]):
        self.node_id = node_id
        self.peers = peers

        # Persistent state — harus bertahan saat crash/restart
        self.current_term = 0
        self.voted_for: Optional[int] = None
        self.log: List[LogEntry] = []

        # Volatile state
        self.role = NodeRole.FOLLOWER
        self.commit_index = 0
        self.last_applied = 0
        self.current_leader: Optional[int] = None

        # Volatile state khusus leader — di-reset saat jadi leader baru
        self.next_index: Dict[int, int] = {}   # index berikutnya yang akan dikirim ke peer
        self.match_index: Dict[int, int] = {}  # index tertinggi yang diketahui cocok di peer

        self._election_timeout = self._new_election_timeout()
        self._last_heartbeat = time.time()

        # Callback ke state machine (LockManager/Queue/Cache mendaftarkan fungsinya di sini)
        self._state_machine_callback: Optional[Callable] = None

        self._tasks: List[asyncio.Task] = []
        self._running = False

        self.rpc_sender = None  # diset oleh MessagePassing

        logger.info(f"RaftNode {self.node_id} initialized, peers={self.peers}")

    
    # PUBLIC API
    

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
        logger.info(f"Node {self.node_id} stopped")

    def register_state_machine(self, callback: Callable):
        #Daftarkan callback yang dipanggil tiap ada log entry committed.
        self._state_machine_callback = callback

    async def submit_command(self, command: Dict) -> Optional[int]:
        """
        Terima perintah dari client dan masukkan ke log.
        Hanya bisa dipanggil di leader.

        Returns:
            index log entry baru, atau None jika bukan leader.
        """
        if self.role != NodeRole.LEADER:
            logger.warning(f"Node {self.node_id} bukan leader, tolak command")
            return None

        new_index = len(self.log) + 1
        entry = LogEntry(term=self.current_term, index=new_index, command=command)
        self.log.append(entry)
        logger.info(f"Leader {self.node_id} appended entry index={new_index}: {command}")

        await self._replicate_to_all()
        return new_index

    async def wait_for_commit(self, index: int, timeout: float = 5.0) -> bool:
        """Tunggu sampai log entry pada index tertentu committed."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.commit_index >= index:
                return True
            await asyncio.sleep(0.01)
        return False

    
    # LEADER ELECTION
    

    def _new_election_timeout(self) -> float:
        """Buat election timeout acak agar node tidak mulai election bersamaan."""
        ms = random.randint(
            config.raft.election_timeout_min,
            config.raft.election_timeout_max
        )
        return ms / 1000.0

    async def _election_timer_loop(self):
        """Pantau heartbeat; mulai election jika timeout terlampaui."""
        while self._running:
            await asyncio.sleep(0.01)

            if self.role == NodeRole.LEADER:
                await self._send_heartbeats()
                await asyncio.sleep(config.raft.heartbeat_interval / 1000.0)
                continue

            elapsed = time.time() - self._last_heartbeat
            if elapsed >= self._election_timeout:
                logger.info(
                    f"Node {self.node_id} election timeout ({elapsed:.3f}s), "
                    f"memulai election di term {self.current_term + 1}"
                )
                await self._start_election()

    async def _start_election(self):
        """
        Mulai pemilihan leader:
        naikkan term → vote sendiri → kirim RequestVote → jadi leader jika menang.
        """
        self.role = NodeRole.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self._last_heartbeat = time.time()
        self._election_timeout = self._new_election_timeout()

        metrics.set_role("candidate")
        metrics.election_total.labels(node_id=str(self.node_id)).inc()

        votes_received = 1  # suara dari diri sendiri
        votes_needed = (len(self.peers) + 1) // 2 + 1

        logger.info(
            f"Node {self.node_id} jadi CANDIDATE term={self.current_term}, "
            f"butuh {votes_needed} suara dari {len(self.peers) + 1} node"
        )

        last_log_index = len(self.log)
        last_log_term = self.log[-1].term if self.log else 0

        vote_request = VoteRequest(
            term=self.current_term,
            candidate_id=self.node_id,
            last_log_index=last_log_index,
            last_log_term=last_log_term
        )

        vote_tasks = [
            self._request_vote_from(peer_id, vote_request)
            for peer_id in self.peers
        ]
        responses = await asyncio.gather(*vote_tasks, return_exceptions=True)

        for response in responses:
            if isinstance(response, Exception):
                continue
            if not isinstance(response, VoteResponse):
                continue

            if response.term > self.current_term:
                await self._become_follower(response.term)
                return

            if response.vote_granted:
                votes_received += 1
                logger.debug(f"Node {self.node_id} dapat suara, total={votes_received}")

        if self.role == NodeRole.CANDIDATE and votes_received >= votes_needed:
            await self._become_leader()
        else:
            logger.info(
                f"Node {self.node_id} kalah election "
                f"(dapat {votes_received} dari {votes_needed} yang dibutuhkan)"
            )
            self.role = NodeRole.FOLLOWER
            metrics.set_role("follower")

    async def _request_vote_from(
        self, peer_id: int, request: VoteRequest
    ) -> Optional[VoteResponse]:
        """Kirim RequestVote ke satu peer, return VoteResponse atau None jika gagal."""
        try:
            if self.rpc_sender is None:
                return None
            response_data = await self.rpc_sender.send_vote_request(peer_id, request)
            if response_data:
                return VoteResponse(**response_data)
        except Exception as e:
            logger.debug(f"RequestVote ke peer {peer_id} gagal: {e}")
        return None

    async def _become_leader(self):
        """Inisialisasi state leader dan kirim heartbeat pertama."""
        self.role = NodeRole.LEADER
        self.current_leader = self.node_id
        metrics.set_role("leader")

        for peer_id in self.peers:
            self.next_index[peer_id] = len(self.log) + 1
            self.match_index[peer_id] = 0

        logger.info(f"*** Node {self.node_id} jadi LEADER di term {self.current_term} ***")
        await self._send_heartbeats()

    async def _become_follower(self, term: int):
        """Mundur jadi follower karena melihat term yang lebih tinggi."""
        logger.info(
            f"Node {self.node_id} mundur jadi FOLLOWER "
            f"(term {self.current_term} → {term})"
        )
        self.current_term = term
        self.role = NodeRole.FOLLOWER
        self.voted_for = None
        self._last_heartbeat = time.time()
        metrics.set_role("follower")

    
    # HANDLE INCOMING RPC (dipanggil oleh communication layer)
    async def handle_vote_request(self, request: VoteRequest) -> VoteResponse:
        """
        Proses RequestVote dari candidate.
        Suara diberikan jika: term candidate ≥ term kita, belum vote di term ini,
        dan log candidate minimal se-update log kita.
        """
        if request.term > self.current_term:
            await self._become_follower(request.term)

        if request.term < self.current_term:
            return VoteResponse(term=self.current_term, vote_granted=False)

        already_voted = (
            self.voted_for is not None and
            self.voted_for != request.candidate_id
        )
        if already_voted:
            logger.debug(
                f"Node {self.node_id} tolak vote untuk {request.candidate_id} "
                f"(sudah vote untuk {self.voted_for})"
            )
            return VoteResponse(term=self.current_term, vote_granted=False)

        my_last_log_index = len(self.log)
        my_last_log_term = self.log[-1].term if self.log else 0

        # Log candidate dianggap up-to-date jika term-nya lebih tinggi,
        # atau term sama tapi index-nya lebih panjang
        candidate_log_ok = (
            request.last_log_term > my_last_log_term or
            (request.last_log_term == my_last_log_term and
             request.last_log_index >= my_last_log_index)
        )

        if not candidate_log_ok:
            logger.debug(
                f"Node {self.node_id} tolak vote untuk {request.candidate_id} "
                f"(log candidate tidak se-update)"
            )
            return VoteResponse(term=self.current_term, vote_granted=False)

        self.voted_for = request.candidate_id
        self._last_heartbeat = time.time()
        logger.info(
            f"Node {self.node_id} VOTE untuk candidate {request.candidate_id} "
            f"di term {request.term}"
        )
        return VoteResponse(term=self.current_term, vote_granted=True)

    async def handle_append_entries(
        self, request: AppendEntriesRequest
    ) -> AppendEntriesResponse:
        """Proses AppendEntries dari leader (replikasi log atau heartbeat)."""
        if request.term < self.current_term:
            return AppendEntriesResponse(term=self.current_term, success=False)

        if request.term > self.current_term:
            self.current_term = request.term
            self.voted_for = None

        self._last_heartbeat = time.time()
        self.role = NodeRole.FOLLOWER
        self.current_leader = request.leader_id
        metrics.set_role("follower")

        # Validasi konsistensi log di prev_log_index
        if request.prev_log_index > 0:
            if len(self.log) < request.prev_log_index:
                logger.debug(
                    f"Node {self.node_id} log terlalu pendek "
                    f"(punya {len(self.log)}, dibutuhkan {request.prev_log_index})"
                )
                return AppendEntriesResponse(term=self.current_term, success=False)

            existing_entry = self.log[request.prev_log_index - 1]
            if existing_entry.term != request.prev_log_term:
                # Konflik: hapus entry dari titik konflik ke depan
                logger.debug(
                    f"Node {self.node_id} konflik log di index {request.prev_log_index}, "
                    f"hapus entries yang konflik"
                )
                self.log = self.log[:request.prev_log_index - 1]
                return AppendEntriesResponse(term=self.current_term, success=False)

        for entry_dict in request.entries:
            entry = LogEntry.from_dict(entry_dict)
            if entry.index <= len(self.log):
                existing = self.log[entry.index - 1]
                if existing.term != entry.term:
                    self.log = self.log[:entry.index - 1]
                    self.log.append(entry)
            else:
                self.log.append(entry)

        if request.leader_commit > self.commit_index:
            self.commit_index = min(request.leader_commit, len(self.log))

        match_index = len(self.log)
        return AppendEntriesResponse(term=self.current_term, success=True, match_index=match_index)

    
    # LOG REPLICATION (Leader only)
    

    async def _send_heartbeats(self):
        """Kirim heartbeat (AppendEntries kosong) ke semua follower."""
        if self.role != NodeRole.LEADER:
            return

        tasks = [self._send_append_entries(peer_id) for peer_id in self.peers]
        await asyncio.gather(*tasks, return_exceptions=True)
        metrics.heartbeat_sent.labels(node_id=str(self.node_id)).inc()

    async def _replicate_to_all(self):
        """Replikasi log entries baru ke semua follower, update commit index jika kuorum tercapai."""
        if self.role != NodeRole.LEADER:
            return

        tasks = [self._send_append_entries(peer_id) for peer_id in self.peers]
        responses = await asyncio.gather(*tasks, return_exceptions=True)

        success_count = 1  # hitung leader sendiri
        for resp in responses:
            if isinstance(resp, AppendEntriesResponse) and resp.success:
                success_count += 1

        quorum = (len(self.peers) + 1) // 2 + 1
        if success_count >= quorum:
            new_commit = len(self.log)
            if new_commit > self.commit_index:
                self.commit_index = new_commit
                logger.info(f"Leader {self.node_id} commit index naik ke {self.commit_index}")

    async def _send_append_entries(
        self, peer_id: int
    ) -> Optional[AppendEntriesResponse]:
        """Kirim AppendEntries ke satu peer."""
        if self.role != NodeRole.LEADER:
            return None

        next_idx = self.next_index.get(peer_id, 1)
        prev_log_index = next_idx - 1
        prev_log_term = 0

        if prev_log_index > 0 and prev_log_index <= len(self.log):
            prev_log_term = self.log[prev_log_index - 1].term

        entries_to_send = []
        if next_idx <= len(self.log):
            entries_to_send = [e.to_dict() for e in self.log[next_idx - 1:]]

        request = AppendEntriesRequest(
            term=self.current_term,
            leader_id=self.node_id,
            prev_log_index=prev_log_index,
            prev_log_term=prev_log_term,
            entries=entries_to_send,
            leader_commit=self.commit_index
        )

        try:
            if self.rpc_sender is None:
                return None
            response_data = await self.rpc_sender.send_append_entries(peer_id, request)
            if not response_data:
                return None

            response = AppendEntriesResponse(**response_data)

            if response.term > self.current_term:
                await self._become_follower(response.term)
                return response

            if response.success:
                self.match_index[peer_id] = response.match_index
                self.next_index[peer_id] = response.match_index + 1
            else:
                # Mundurkan next_index dan coba ulang
                self.next_index[peer_id] = max(1, self.next_index[peer_id] - 1)

            return response

        except Exception as e:
            logger.debug(f"AppendEntries ke peer {peer_id} gagal: {e}")
            return None

    
    # APPLY COMMITTED ENTRIES
    async def _apply_committed_entries_loop(self):
        """
        Eksekusi log entries yang sudah committed ke state machine.
        Menjembatani Raft dengan LockManager/Queue/Cache via callback.
        """
        while self._running:
            await asyncio.sleep(0.005)

            while self.last_applied < self.commit_index:
                self.last_applied += 1
                entry = self.log[self.last_applied - 1]
                entry.committed = True

                logger.debug(
                    f"Node {self.node_id} apply entry index={entry.index}: {entry.command}"
                )

                if self._state_machine_callback:
                    try:
                        await self._state_machine_callback(entry.command)
                    except Exception as e:
                        logger.error(f"State machine error pada entry {entry.index}: {e}")

    
    # STATUS & DEBUG
    

    def get_status(self) -> Dict:
        """Return status node untuk health check endpoint."""
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