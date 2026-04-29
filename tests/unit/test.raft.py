import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock

from src.consensus.raft import (
    RaftNode, NodeRole, LogEntry,
    VoteRequest, VoteResponse,
    AppendEntriesRequest, AppendEntriesResponse
)



# FIXTURES
def make_node(node_id=1, peers=None) -> RaftNode:
    """Buat RaftNode tanpa RPC sender (untuk testing)."""
    if peers is None:
        peers = [2, 3]
    node = RaftNode(node_id=node_id, peers=peers)
    node.rpc_sender = None  # tidak ada network
    return node



# TEST: Initial State
class TestInitialState:
    def test_starts_as_follower(self):
        node = make_node()
        assert node.role == NodeRole.FOLLOWER

    def test_initial_term_is_zero(self):
        node = make_node()
        assert node.current_term == 0

    def test_initial_log_is_empty(self):
        node = make_node()
        assert len(node.log) == 0

    def test_initial_commit_index_is_zero(self):
        node = make_node()
        assert node.commit_index == 0

    def test_voted_for_is_none(self):
        node = make_node()
        assert node.voted_for is None

    def test_no_leader_initially(self):
        node = make_node()
        assert node.current_leader is None
        assert not node.is_leader()



# TEST: handle_vote_request
class TestVoteRequest:
    @pytest.mark.asyncio
    async def test_grant_vote_to_valid_candidate(self):
        """Node harus kasih suara ke candidate dengan term lebih tinggi."""
        node = make_node(node_id=2)

        request = VoteRequest(
            term=1,
            candidate_id=1,
            last_log_index=0,
            last_log_term=0,
        )
        response = await node.handle_vote_request(request)

        assert response.vote_granted is True
        assert node.voted_for == 1

    @pytest.mark.asyncio
    async def test_reject_vote_with_stale_term(self):
        """Tolak request dari candidate dengan term lebih rendah."""
        node = make_node(node_id=2)
        node.current_term = 5  # node sudah di term 5

        request = VoteRequest(
            term=3,  # candidate ketinggalan
            candidate_id=1,
            last_log_index=0,
            last_log_term=0,
        )
        response = await node.handle_vote_request(request)

        assert response.vote_granted is False

    @pytest.mark.asyncio
    async def test_reject_double_vote_same_term(self):
        """Tidak boleh vote dua kali di term yang sama."""
        node = make_node(node_id=3)
        node.current_term = 1
        node.voted_for = 1  # sudah vote untuk node 1

        request = VoteRequest(
            term=1,
            candidate_id=2,  # request dari node 2
            last_log_index=0,
            last_log_term=0,
        )
        response = await node.handle_vote_request(request)

        assert response.vote_granted is False
        assert node.voted_for == 1  # tetap untuk node 1

    @pytest.mark.asyncio
    async def test_vote_same_candidate_twice_ok(self):
        """Boleh vote untuk candidate yang sama dua kali (idempotent)."""
        node = make_node(node_id=3)
        node.current_term = 1
        node.voted_for = 1  # sudah vote untuk node 1

        request = VoteRequest(
            term=1,
            candidate_id=1,  # candidate yang sama
            last_log_index=0,
            last_log_term=0,
        )
        response = await node.handle_vote_request(request)

        assert response.vote_granted is True

    @pytest.mark.asyncio
    async def test_reject_if_candidate_log_outdated(self):
        """
        Tolak jika log candidate lebih pendek dari log node ini.
        Ini 'election restriction' Raft - mencegah data hilang.
        """
        node = make_node(node_id=2)
        # Node 2 punya 3 log entries di term 2
        node.log = [
            LogEntry(term=1, index=1, command={"op": "SET", "k": "a"}),
            LogEntry(term=2, index=2, command={"op": "SET", "k": "b"}),
            LogEntry(term=2, index=3, command={"op": "SET", "k": "c"}),
        ]
        node.current_term = 2

        request = VoteRequest(
            term=3,
            candidate_id=1,
            last_log_index=1,  # candidate hanya punya 1 entry
            last_log_term=1,
        )
        response = await node.handle_vote_request(request)

        assert response.vote_granted is False



# TEST: handle_append_entries
class TestAppendEntries:
    @pytest.mark.asyncio
    async def test_heartbeat_resets_election_timeout(self):
        """Heartbeat (entries kosong) harus update last_heartbeat."""
        import time
        node = make_node(node_id=2)
        old_heartbeat = node._last_heartbeat

        await asyncio.sleep(0.01)

        request = AppendEntriesRequest(
            term=1,
            leader_id=1,
            prev_log_index=0,
            prev_log_term=0,
            entries=[],  # kosong = heartbeat
            leader_commit=0,
        )
        response = await node.handle_append_entries(request)

        assert response.success is True
        assert node._last_heartbeat > old_heartbeat
        assert node.current_leader == 1

    @pytest.mark.asyncio
    async def test_reject_append_with_stale_term(self):
        """Tolak AppendEntries dari leader dengan term ketinggalan."""
        node = make_node(node_id=2)
        node.current_term = 5

        request = AppendEntriesRequest(
            term=3,  # term ketinggalan
            leader_id=1,
            prev_log_index=0,
            prev_log_term=0,
            entries=[],
            leader_commit=0,
        )
        response = await node.handle_append_entries(request)

        assert response.success is False
        assert response.term == 5

    @pytest.mark.asyncio
    async def test_append_new_entries(self):
        """Follower harus menambahkan entries baru ke log-nya."""
        node = make_node(node_id=2)

        entries = [
            {"term": 1, "index": 1, "command": {"op": "LOCK", "key": "db"}, "committed": False},
            {"term": 1, "index": 2, "command": {"op": "LOCK", "key": "file"}, "committed": False},
        ]

        request = AppendEntriesRequest(
            term=1,
            leader_id=1,
            prev_log_index=0,
            prev_log_term=0,
            entries=entries,
            leader_commit=0,
        )
        response = await node.handle_append_entries(request)

        assert response.success is True
        assert len(node.log) == 2
        assert node.log[0].command["op"] == "LOCK"

    @pytest.mark.asyncio
    async def test_update_commit_index(self):
        """Commit index follower harus update mengikuti leader."""
        node = make_node(node_id=2)
        node.log = [
            LogEntry(term=1, index=1, command={"op": "LOCK", "key": "db"}),
            LogEntry(term=1, index=2, command={"op": "LOCK", "key": "file"}),
        ]

        request = AppendEntriesRequest(
            term=1,
            leader_id=1,
            prev_log_index=2,
            prev_log_term=1,
            entries=[],
            leader_commit=2,  # leader sudah commit index 2
        )
        response = await node.handle_append_entries(request)

        assert response.success is True
        assert node.commit_index == 2



# TEST: submit_command (Leader only)
class TestSubmitCommand:
    @pytest.mark.asyncio
    async def test_non_leader_rejects_command(self):
        """Hanya leader yang boleh menerima command dari client."""
        node = make_node(node_id=2)
        # node masih follower

        result = await node.submit_command({"op": "LOCK", "key": "db"})

        assert result is None

    @pytest.mark.asyncio
    async def test_leader_appends_command_to_log(self):
        """Leader harus menambahkan command ke log-nya."""
        node = make_node(node_id=1)
        node.role = NodeRole.LEADER
        node.current_term = 1
        # Tidak ada RPC sender, jadi replicate tidak akan kemana-mana
        # tapi command tetap harus masuk ke log

        result = await node.submit_command({"op": "LOCK", "key": "db"})

        assert result == 1  # index pertama
        assert len(node.log) == 1
        assert node.log[0].command["op"] == "LOCK"



# TEST: LogEntry
class TestLogEntry:
    def test_to_dict_and_back(self):
        """LogEntry harus bisa dikonversi ke dict dan balik lagi."""
        entry = LogEntry(
            term=2,
            index=5,
            command={"op": "UNLOCK", "key": "db"},
            committed=True
        )
        d = entry.to_dict()
        restored = LogEntry.from_dict(d)

        assert restored.term == entry.term
        assert restored.index == entry.index
        assert restored.command == entry.command
        assert restored.committed == entry.committed

    def test_get_status_returns_dict(self):
        node = make_node(node_id=1)
        node.role = NodeRole.LEADER
        node.current_term = 3

        status = node.get_status()

        assert status["node_id"] == 1
        assert status["role"] == "leader"
        assert status["current_term"] == 3