
import asyncio
import time
import pytest
from unittest.mock import AsyncMock, MagicMock

from src.nodes.cache_node import CacheNode, MESIState, CacheEntry, LRUCache, LFUCache


# HELPERS

def make_mock_raft(is_leader=True):
    raft = MagicMock()
    raft.is_leader.return_value = is_leader
    raft.submit_command = AsyncMock(return_value=1)
    raft.wait_for_commit = AsyncMock(return_value=True)
    raft.register_state_machine = MagicMock()
    return raft


async def make_cache_node(is_leader=True) -> CacheNode:
    raft = make_mock_raft(is_leader)
    cn = CacheNode(raft)
    cn._redis = None
    cn._running = True
    return cn


# TEST: MESI State Transitions

class TestMESIStateTransitions:

    @pytest.mark.asyncio
    async def test_entry_baru_dapat_state_exclusive(self):
        """Key baru yang di-set harus mendapat state EXCLUSIVE."""
        cn = await make_cache_node()
        await cn._apply_command({
            "op": "CACHE_SET", "key": "user:1",
            "value": {"name": "Budi"}, "ttl": None, "node_id": 1
        })
        entry = cn._cache.get("user:1")
        assert entry is not None
        assert entry.state == MESIState.EXCLUSIVE

    @pytest.mark.asyncio
    async def test_update_key_existing_dapat_state_modified(self):
        """Key yang diupdate harus berubah ke state MODIFIED."""
        cn = await make_cache_node()
        await cn._apply_command({
            "op": "CACHE_SET", "key": "user:1",
            "value": {"name": "Budi"}, "ttl": None, "node_id": 1
        })
        await cn._apply_command({
            "op": "CACHE_SET", "key": "user:1",
            "value": {"name": "Budi Updated"}, "ttl": None, "node_id": 1
        })
        entry = cn._cache.get("user:1")
        assert entry.state == MESIState.MODIFIED

    @pytest.mark.asyncio
    async def test_invalidate_ubah_state_ke_invalid(self):
        """Cache invalidation harus mengubah state entry menjadi INVALID."""
        cn = await make_cache_node()
        await cn._apply_command({
            "op": "CACHE_SET", "key": "user:1",
            "value": {"name": "Budi"}, "ttl": None, "node_id": 1
        })
        await cn._apply_command({
            "op": "CACHE_INVALIDATE", "key": "user:1", "node_id": 1
        })
        entry = cn._cache.get("user:1")
        assert entry is None or entry.state == MESIState.INVALID


# TEST: Get & Set

class TestGetSet:

    @pytest.mark.asyncio
    async def test_get_return_hit_setelah_set(self):
        """Setelah set, get harus return hit=True dengan nilai yang benar."""
        cn = await make_cache_node()
        await cn._apply_command({
            "op": "CACHE_SET", "key": "config:timeout",
            "value": 30, "ttl": None, "node_id": 1
        })
        hit, value = await cn.get("config:timeout")
        assert hit is True
        assert value == 30

    @pytest.mark.asyncio
    async def test_get_return_miss_untuk_key_tidak_ada(self):
        """Get key yang tidak ada harus return hit=False."""
        cn = await make_cache_node()
        hit, value = await cn.get("nonexistent:key")
        assert hit is False
        assert value is None

    @pytest.mark.asyncio
    async def test_get_return_miss_setelah_invalidate(self):
        """Setelah invalidasi, get harus return miss."""
        cn = await make_cache_node()
        await cn._apply_command({
            "op": "CACHE_SET", "key": "user:1",
            "value": {"name": "Budi"}, "ttl": None, "node_id": 1
        })
        await cn._apply_command({
            "op": "CACHE_INVALIDATE", "key": "user:1", "node_id": 1
        })
        hit, value = await cn.get("user:1")
        assert hit is False

    @pytest.mark.asyncio
    async def test_ttl_expired_return_miss(self):
        """Entry yang sudah expired tidak boleh dikembalikan saat get."""
        cn = await make_cache_node()
        await cn._apply_command({
            "op": "CACHE_SET", "key": "session:abc",
            "value": "token-xyz", "ttl": 1, "node_id": 1
        })
        entry = cn._cache.get("session:abc")
        entry.created_at = time.time() - 10

        hit, value = await cn.get("session:abc")
        assert hit is False

    @pytest.mark.asyncio
    async def test_hit_count_naik_setiap_cache_hit(self):
        """Setiap cache hit harus menaikkan hit_count."""
        cn = await make_cache_node()
        await cn._apply_command({
            "op": "CACHE_SET", "key": "k", "value": "v", "ttl": None, "node_id": 1
        })
        await cn.get("k")
        await cn.get("k")
        await cn.get("k")
        assert cn._hit_count == 3

    @pytest.mark.asyncio
    async def test_miss_count_naik_setiap_cache_miss(self):
        """Setiap cache miss harus menaikkan miss_count."""
        cn = await make_cache_node()
        await cn.get("tidak-ada-1")
        await cn.get("tidak-ada-2")
        assert cn._miss_count == 2


# TEST: LRU Cache

class TestLRUCache:

    def make_entry(self, key: str, value=None) -> CacheEntry:
        return CacheEntry(
            key=key, value=value or key,
            state=MESIState.EXCLUSIVE,
            last_access=time.time(),
            access_count=0,
            created_at=time.time(),
        )

    def test_put_dan_get_basic(self):
        """LRU harus bisa menyimpan dan mengambil entry."""
        lru = LRUCache(max_size=3)
        lru.put("a", self.make_entry("a", "value-a"))
        entry = lru.get("a")
        assert entry is not None
        assert entry.value == "value-a"

    def test_evict_entry_terlama_saat_penuh(self):
        """Saat cache penuh, entry yang paling lama tidak diakses harus dibuang."""
        lru = LRUCache(max_size=3)
        lru.put("a", self.make_entry("a"))
        lru.put("b", self.make_entry("b"))
        lru.put("c", self.make_entry("c"))

        lru.get("a")
        lru.get("b")

        evicted = lru.put("d", self.make_entry("d"))
        assert evicted == "c"

    def test_get_pindahkan_ke_akhir(self):
        """Akses entry harus memindahkannya ke posisi terbaru (tidak dievict duluan)."""
        lru = LRUCache(max_size=2)
        lru.put("a", self.make_entry("a"))
        lru.put("b", self.make_entry("b"))
        lru.get("a")
        evicted = lru.put("c", self.make_entry("c"))
        assert evicted == "b"
        assert lru.get("a") is not None

    def test_delete_hapus_entry(self):
        """Delete harus menghapus entry dari cache."""
        lru = LRUCache(max_size=3)
        lru.put("a", self.make_entry("a"))
        lru.delete("a")
        assert lru.get("a") is None

    def test_len_return_jumlah_entry(self):
        """len() harus mengembalikan jumlah entry yang ada."""
        lru = LRUCache(max_size=10)
        for i in range(5):
            lru.put(f"key-{i}", self.make_entry(f"key-{i}"))
        assert len(lru) == 5


# TEST: LFU Cache

class TestLFUCache:

    def make_entry(self, key: str) -> CacheEntry:
        return CacheEntry(
            key=key, value=key,
            state=MESIState.EXCLUSIVE,
            last_access=time.time(),
            access_count=0,
            created_at=time.time(),
        )

    def test_evict_entry_frekuensi_terendah(self):
        """LFU harus membuang entry yang paling jarang diakses."""
        lfu = LFUCache(max_size=3)
        lfu.put("a", self.make_entry("a"))
        lfu.put("b", self.make_entry("b"))
        lfu.put("c", self.make_entry("c"))

        lfu.get("a")
        lfu.get("a")
        lfu.get("b")

        evicted = lfu.put("d", self.make_entry("d"))
        assert evicted == "c"

    def test_delete_hapus_entry(self):
        """Delete harus menghapus entry dari LFU cache."""
        lfu = LFUCache(max_size=3)
        lfu.put("a", self.make_entry("a"))
        lfu.delete("a")
        assert lfu.get("a") is None


# TEST: Cache Status

class TestCacheStatus:

    @pytest.mark.asyncio
    async def test_status_hit_rate_akurat(self):
        """hit_rate dalam status harus dihitung dengan benar."""
        cn = await make_cache_node()
        await cn._apply_command({
            "op": "CACHE_SET", "key": "k", "value": "v", "ttl": None, "node_id": 1
        })

        await cn.get("k")
        await cn.get("k")
        await cn.get("tidak-ada")

        status = cn.get_status()
        assert status["hit_count"] == 2
        assert status["miss_count"] == 1
        assert abs(status["hit_rate"] - (2/3)) < 0.01

    @pytest.mark.asyncio
    async def test_status_size_sesuai_isi_cache(self):
        """size dalam status harus sesuai dengan jumlah entry di cache."""
        cn = await make_cache_node()
        for i in range(5):
            await cn._apply_command({
                "op": "CACHE_SET", "key": f"key:{i}",
                "value": i, "ttl": None, "node_id": 1
            })
        status = cn.get_status()
        assert status["size"] == 5