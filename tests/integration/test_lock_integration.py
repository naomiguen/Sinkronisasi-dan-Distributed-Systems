import asyncio
import time
import pytest
import pytest_asyncio

from unittest.mock import AsyncMock, MagicMock

from src.nodes.lock_manager import (
    LockManager, LockRequest, LockType, LockStatus
)


# HELPERS

def make_mock_raft(is_leader=True):
    """Buat mock RaftNode yang bisa dikontrol per test."""
    raft = MagicMock()
    raft.is_leader.return_value = is_leader
    raft.submit_command = AsyncMock(return_value=1)
    raft.wait_for_commit = AsyncMock(return_value=True)
    raft.register_state_machine = MagicMock()
    return raft


async def make_lock_manager(is_leader=True) -> LockManager:
    """Buat LockManager dengan mock Raft tanpa koneksi Redis nyata."""
    raft = make_mock_raft(is_leader)
    lm = LockManager(raft)
    lm._redis = None  # skip Redis untuk test ini
    lm._running = True
    return lm


# TEST: Acquire & Release dasar

class TestAcquireRelease:

    @pytest.mark.asyncio
    async def test_acquire_exclusive_lock_berhasil(self):
        """Client pertama harus bisa dapat exclusive lock di resource kosong."""
        lm = await make_lock_manager()
        req = LockRequest(resource="db", lock_type=LockType.EXCLUSIVE, client_id="A")

        await lm.apply_command({
            "op": "LOCK_ACQUIRE", "resource": "db",
            "lock_type": "exclusive", "client_id": "A",
            "node_id": 1, "ttl": 30
        })

        result = await lm.acquire(req)
        assert result.status == LockStatus.GRANTED

    @pytest.mark.asyncio
    async def test_release_lock_berhasil(self):
        """Lock yang sudah diperoleh harus bisa dilepas."""
        lm = await make_lock_manager()

        await lm.apply_command({
            "op": "LOCK_ACQUIRE", "resource": "db",
            "lock_type": "exclusive", "client_id": "A",
            "node_id": 1, "ttl": 30
        })
        await lm.apply_command({"op": "LOCK_RELEASE", "resource": "db", "client_id": "A"})

        assert "A" not in lm._get_holders("db")

    @pytest.mark.asyncio
    async def test_exclusive_lock_ditolak_saat_ada_holder(self):
        """Client B tidak boleh dapat exclusive lock jika A sudah memegangnya."""
        lm = await make_lock_manager()

        await lm.apply_command({
            "op": "LOCK_ACQUIRE", "resource": "db",
            "lock_type": "exclusive", "client_id": "A",
            "node_id": 1, "ttl": 30
        })

        can = lm._can_acquire("db", LockType.EXCLUSIVE, "B")
        assert can is False

    @pytest.mark.asyncio
    async def test_shared_lock_bisa_dipegang_banyak_client(self):
        """Beberapa client bisa memegang shared lock secara bersamaan."""
        lm = await make_lock_manager()

        for client in ["A", "B", "C"]:
            await lm.apply_command({
                "op": "LOCK_ACQUIRE", "resource": "config",
                "lock_type": "shared", "client_id": client,
                "node_id": 1, "ttl": 30
            })

        assert lm._can_acquire("config", LockType.SHARED, "D") is True

    @pytest.mark.asyncio
    async def test_exclusive_ditolak_jika_ada_shared(self):
        """Exclusive lock tidak boleh diperoleh jika ada shared lock aktif."""
        lm = await make_lock_manager()

        await lm.apply_command({
            "op": "LOCK_ACQUIRE", "resource": "config",
            "lock_type": "shared", "client_id": "A",
            "node_id": 1, "ttl": 30
        })

        can = lm._can_acquire("config", LockType.EXCLUSIVE, "B")
        assert can is False

    @pytest.mark.asyncio
    async def test_lock_tersedia_setelah_release(self):
        """Setelah lock dilepas, client lain harus bisa mengambilnya."""
        lm = await make_lock_manager()

        await lm.apply_command({
            "op": "LOCK_ACQUIRE", "resource": "db",
            "lock_type": "exclusive", "client_id": "A",
            "node_id": 1, "ttl": 30
        })
        await lm.apply_command({"op": "LOCK_RELEASE", "resource": "db", "client_id": "A"})

        can = lm._can_acquire("db", LockType.EXCLUSIVE, "B")
        assert can is True

    @pytest.mark.asyncio
    async def test_non_leader_ditolak(self):
        """Node yang bukan leader harus menolak semua request acquire."""
        lm = await make_lock_manager(is_leader=False)
        req = LockRequest(resource="db", lock_type=LockType.EXCLUSIVE, client_id="A")
        result = await lm.acquire(req)
        assert result.status == LockStatus.DENIED


# TEST: TTL & Expiry

class TestTTLExpiry:

    @pytest.mark.asyncio
    async def test_lock_expired_tidak_dianggap_aktif(self):
        """Lock yang sudah melewati TTL-nya tidak boleh dihitung sebagai holder aktif."""
        lm = await make_lock_manager()

        await lm.apply_command({
            "op": "LOCK_ACQUIRE", "resource": "db",
            "lock_type": "exclusive", "client_id": "A",
            "node_id": 1, "ttl": 1
        })

        lock = lm._get_lock("db", "A")
        assert lock is not None

        lock.expires_at = time.time() - 1

        can = lm._can_acquire("db", LockType.EXCLUSIVE, "B")
        assert can is True

    @pytest.mark.asyncio
    async def test_get_all_locks_skip_expired(self):
        """get_all_locks() hanya mengembalikan lock yang belum expired."""
        lm = await make_lock_manager()

        await lm.apply_command({
            "op": "LOCK_ACQUIRE", "resource": "db",
            "lock_type": "exclusive", "client_id": "A",
            "node_id": 1, "ttl": 30
        })
        await lm.apply_command({
            "op": "LOCK_ACQUIRE", "resource": "cache",
            "lock_type": "exclusive", "client_id": "B",
            "node_id": 1, "ttl": 1
        })

        expired_lock = lm._get_lock("cache", "B")
        expired_lock.expires_at = time.time() - 1

        all_locks = lm.get_all_locks()
        assert "db" in all_locks
        assert "cache" not in all_locks


# TEST: Deadlock Detection

class TestDeadlockDetection:

    @pytest.mark.asyncio
    async def test_deteksi_siklus_dua_client(self):
        """Deteksi deadlock: A menunggu B, B menunggu A → siklus."""
        lm = await make_lock_manager()

        lm._deadlock_detector.add_wait("A", {"B"})
        lm._deadlock_detector.add_wait("B", {"A"})

        cycle = lm._deadlock_detector.has_cycle()
        assert cycle is not None
        assert len(cycle) >= 2

    @pytest.mark.asyncio
    async def test_tidak_ada_siklus_jika_tidak_deadlock(self):
        """Tidak ada siklus jika wait-for graph adalah DAG (tidak ada cycle)."""
        lm = await make_lock_manager()

        lm._deadlock_detector.add_wait("A", {"B"})
        lm._deadlock_detector.add_wait("B", {"C"})

        cycle = lm._deadlock_detector.has_cycle()
        assert cycle is None

    @pytest.mark.asyncio
    async def test_deteksi_siklus_tiga_client(self):
        """Deadlock tiga pihak: A→B→C→A."""
        lm = await make_lock_manager()

        lm._deadlock_detector.add_wait("A", {"B"})
        lm._deadlock_detector.add_wait("B", {"C"})
        lm._deadlock_detector.add_wait("C", {"A"})

        cycle = lm._deadlock_detector.has_cycle()
        assert cycle is not None

    @pytest.mark.asyncio
    async def test_remove_wait_menghilangkan_siklus(self):
        """Setelah remove_wait, siklus seharusnya tidak terdeteksi lagi."""
        lm = await make_lock_manager()

        lm._deadlock_detector.add_wait("A", {"B"})
        lm._deadlock_detector.add_wait("B", {"A"})

        lm._deadlock_detector.remove_wait("A")
        cycle = lm._deadlock_detector.has_cycle()
        assert cycle is None


# TEST: Concurrent Acquire (simulasi race condition)

class TestConcurrentLock:

    @pytest.mark.asyncio
    async def test_hanya_satu_winner_dari_banyak_acquirer(self):
        """
        Simulasi 5 client berebut exclusive lock bersamaan.
        Hanya satu yang boleh berhasil di satu waktu.
        """
        lm = await make_lock_manager()

        winners = []

        async def try_acquire(client_id: str):
            can = lm._can_acquire("shared-resource", LockType.EXCLUSIVE, client_id)
            if can:
                await lm.apply_command({
                    "op": "LOCK_ACQUIRE",
                    "resource": "shared-resource",
                    "lock_type": "exclusive",
                    "client_id": client_id,
                    "node_id": 1,
                    "ttl": 30,
                })
                winners.append(client_id)

        clients = [f"client-{i}" for i in range(5)]
        await asyncio.gather(*[try_acquire(c) for c in clients])

        assert len(winners) == 1
        holders = lm._get_holders("shared-resource")
        assert len(holders) == 1