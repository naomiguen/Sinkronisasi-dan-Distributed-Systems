import asyncio
import json
import logging
import time
from collections import OrderedDict
from dataclasses import dataclass, asdict
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

import redis.asyncio as aioredis

from src.utils.config import config
from src.utils.metrics import metrics

logger = logging.getLogger(__name__)


class MESIState(Enum):
    # Protokol koherensi cache: Modified, Exclusive, Shared, Invalid
    MODIFIED  = "M"  # Diubah lokal, belum sinkron ke node lain
    EXCLUSIVE = "E"  # Hanya ada di node ini, masih sinkron
    SHARED    = "S"  # Ada di beberapa node, read-only
    INVALID   = "I"  # Data tidak valid, harus di-fetch ulang


@dataclass
class CacheEntry:
    key: str
    value: Any
    state: MESIState
    last_access: float
    access_count: int
    created_at: float
    ttl: Optional[int] = None  # None berarti tidak pernah expired

    def is_expired(self) -> bool:
        if self.ttl is None:
            return False
        return time.time() > self.created_at + self.ttl

    def is_valid(self) -> bool:
        # Entry valid jika state bukan INVALID dan belum expired
        return self.state != MESIState.INVALID and not self.is_expired()

    def to_dict(self) -> dict:
        d = asdict(self)
        d["state"] = self.state.value
        return d


class LRUCache:
    """Cache dengan eviction Least Recently Used menggunakan OrderedDict."""

    def __init__(self, max_size: int):
        self._max_size = max_size
        self._store: OrderedDict[str, CacheEntry] = OrderedDict()

    def get(self, key: str) -> Optional[CacheEntry]:
        # Pindahkan ke akhir (most-recently-used) setiap kali diakses
        if key not in self._store:
            return None
        self._store.move_to_end(key)
        entry = self._store[key]
        entry.last_access = time.time()
        entry.access_count += 1
        return entry

    def put(self, key: str, entry: CacheEntry) -> Optional[str]:
        # Kembalikan key yang di-evict jika cache penuh, atau None
        evicted_key = None
        if key in self._store:
            self._store.move_to_end(key)
        else:
            if len(self._store) >= self._max_size:
                evicted_key, _ = self._store.popitem(last=False)  # Hapus LRU (paling depan)
        self._store[key] = entry
        return evicted_key

    def delete(self, key: str) -> bool:
        if key in self._store:
            del self._store[key]
            return True
        return False

    def invalidate(self, key: str) -> bool:
        # Tandai INVALID tanpa menghapus entry dari memori
        if key in self._store:
            self._store[key].state = MESIState.INVALID
            return True
        return False

    def __contains__(self, key: str) -> bool:
        return key in self._store

    def __len__(self) -> int:
        return len(self._store)

    def items(self):
        return self._store.items()


class LFUCache:
    """Cache dengan eviction Least Frequently Used; evict entry dengan access_count terendah."""

    def __init__(self, max_size: int):
        self._max_size = max_size
        self._store: Dict[str, CacheEntry] = {}
        self._freq: Dict[str, int] = {}  # Frekuensi akses per key

    def get(self, key: str) -> Optional[CacheEntry]:
        if key not in self._store:
            return None
        entry = self._store[key]
        entry.last_access = time.time()
        entry.access_count += 1
        self._freq[key] = entry.access_count
        return entry

    def put(self, key: str, entry: CacheEntry) -> Optional[str]:
        evicted_key = None
        if key not in self._store and len(self._store) >= self._max_size:
            # Evict key dengan frekuensi terendah
            evicted_key = min(self._freq, key=self._freq.get)
            del self._store[evicted_key]
            del self._freq[evicted_key]
        self._store[key] = entry
        self._freq[key] = entry.access_count
        return evicted_key

    def delete(self, key: str) -> bool:
        if key in self._store:
            del self._store[key]
            self._freq.pop(key, None)
            return True
        return False

    def invalidate(self, key: str) -> bool:
        if key in self._store:
            self._store[key].state = MESIState.INVALID
            return True
        return False

    def __contains__(self, key: str) -> bool:
        return key in self._store

    def __len__(self) -> int:
        return len(self._store)

    def items(self):
        return self._store.items()


class CacheNode:
    """
    Node cache terdistribusi berbasis Raft dengan protokol koherensi MESI.
    Set/invalidate direplikasi via Raft log; get dilayani langsung dari memori lokal.
    Invalidasi antar-node disebarkan lewat Redis Pub/Sub.
    """

    def __init__(self, raft_node):
        self.raft = raft_node
        self.node_id = config.node.node_id

        # Pilih policy eviction dari konfigurasi (default LRU)
        policy = config.cache.replacement_policy.upper()
        if policy == "LFU":
            self._cache = LFUCache(config.cache.max_size)
        else:
            self._cache = LRUCache(config.cache.max_size)

        self._redis: Optional[aioredis.Redis] = None
        self._running = False
        self._tasks: List[asyncio.Task] = []

        self._hit_count = 0
        self._miss_count = 0

        self.raft.register_state_machine(self._apply_command)

    async def start(self):
        self._redis = await aioredis.from_url(
            config.redis.url,
            max_connections=config.redis.max_connections,
            decode_responses=True,
        )
        self._running = True
        self._tasks = [
            asyncio.create_task(self._expiry_loop()),
            asyncio.create_task(self._metrics_loop()),
            asyncio.create_task(self._subscribe_invalidations()),  # Dengarkan invalidasi dari node lain
        ]
        logger.info(
            f"CacheNode {self.node_id} started "
            f"(protocol={config.cache.protocol}, "
            f"policy={config.cache.replacement_policy})"
        )

    async def stop(self):
        self._running = False
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        if self._redis:
            await self._redis.aclose()

    async def get(self, key: str) -> Tuple[bool, Any]:
        # Kembalikan (True, value) jika hit, (False, None) jika miss atau expired/invalid
        entry = self._cache.get(key)
        if entry is None or not entry.is_valid():
            self._miss_count += 1
            metrics.record_cache_miss()
            return False, None

        self._hit_count += 1
        metrics.record_cache_hit()
        return True, entry.value

    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        # Submit ke Raft log; hanya leader yang boleh menulis
        if not self.raft.is_leader():
            return False

        command = {
            "op": "CACHE_SET",
            "key": key,
            "value": value,
            "ttl": ttl,
            "node_id": self.node_id,
        }

        log_index = await self.raft.submit_command(command)
        if log_index is None:
            return False

        committed = await self.raft.wait_for_commit(log_index, timeout=5.0)
        return committed

    async def invalidate(self, key: str) -> bool:
        # Replikasi invalidasi via Raft, lalu broadcast ke node lain via Pub/Sub
        if not self.raft.is_leader():
            return False

        command = {
            "op": "CACHE_INVALIDATE",
            "key": key,
            "node_id": self.node_id,
        }

        log_index = await self.raft.submit_command(command)
        if log_index is None:
            return False

        await self.raft.wait_for_commit(log_index, timeout=5.0)
        await self._broadcast_invalidation(key)
        return True

    async def _apply_command(self, command: dict):
        # Dipanggil Raft setelah commit; dijalankan di semua node
        op = command.get("op")

        if op == "CACHE_SET":
            key = command["key"]
            now = time.time()
            entry = CacheEntry(
                key=key,
                value=command["value"],
                state=MESIState.EXCLUSIVE,
                last_access=now,
                access_count=0,
                created_at=now,
                ttl=command.get("ttl"),
            )
            # Jika key sudah ada, tandai MODIFIED karena nilainya diubah
            if key in self._cache:
                entry.state = MESIState.MODIFIED

            evicted = self._cache.put(key, entry)
            if evicted:
                logger.debug(f"Cache evicted: {evicted}")

            await self._persist_to_redis(key, entry)
            metrics.cache_size.labels(node_id=str(self.node_id)).set(len(self._cache))

        elif op == "CACHE_INVALIDATE":
            key = command["key"]
            self._cache.invalidate(key)
            await self._delete_from_redis(key)
            metrics.cache_invalidations.labels(node_id=str(self.node_id)).inc()
            metrics.cache_size.labels(node_id=str(self.node_id)).set(len(self._cache))

        elif op == "CACHE_SHARED":
            # Transisi EXCLUSIVE → SHARED saat node lain juga membaca key ini
            key = command["key"]
            entry = self._cache.get(key)
            if entry and entry.state == MESIState.EXCLUSIVE:
                entry.state = MESIState.SHARED

    async def _transition_state(self, key: str, new_state: MESIState):
        # Helper eksplisit untuk transisi state MESI dengan logging
        entry = self._cache.get(key)
        if entry:
            old_state = entry.state
            entry.state = new_state
            logger.debug(f"MESI transition [{key}]: {old_state.value} -> {new_state.value}")

    async def _broadcast_invalidation(self, key: str):
        # Publish ke channel Redis agar semua node menginvalidasi key yang sama
        if not self._redis:
            return
        payload = json.dumps({"key": key, "from_node": self.node_id})
        await self._redis.publish("cache:invalidate", payload)

    async def _subscribe_invalidations(self):
        # Dengarkan invalidasi dari node lain; skip jika pesan berasal dari node sendiri
        if not self._redis:
            return
        try:
            pubsub = self._redis.pubsub()
            await pubsub.subscribe("cache:invalidate")

            async for message in pubsub.listen():
                if not self._running:
                    break
                if message["type"] != "message":
                    continue
                data = json.loads(message["data"])
                key = data["key"]
                from_node = data.get("from_node")

                if from_node != self.node_id:
                    self._cache.invalidate(key)
                    logger.debug(f"Node {self.node_id} invalidate [{key}] dari node {from_node}")
        except Exception as e:
            if self._running:
                logger.error(f"Error subscribe invalidations: {e}")

    async def _persist_to_redis(self, key: str, entry: CacheEntry):
        if not self._redis:
            return
        redis_key = f"cache:{key}"
        await self._redis.set(redis_key, json.dumps(entry.to_dict()))
        if entry.ttl:
            await self._redis.expire(redis_key, entry.ttl)

    async def _delete_from_redis(self, key: str):
        if not self._redis:
            return
        await self._redis.delete(f"cache:{key}")

    async def _expiry_loop(self):
        # Setiap 10 detik, hapus entry yang sudah expired dari cache dan Redis
        while self._running:
            await asyncio.sleep(10.0)
            expired_keys = [
                key for key, entry in self._cache.items()
                if entry.is_expired()
            ]
            for key in expired_keys:
                self._cache.delete(key)
                await self._delete_from_redis(key)
            if expired_keys:
                logger.debug(f"Expired {len(expired_keys)} cache entries")

    async def _metrics_loop(self):
        # Setiap 15 detik, log statistik hit rate
        while self._running:
            await asyncio.sleep(15.0)
            total = self._hit_count + self._miss_count
            if total > 0:
                hit_rate = self._hit_count / total * 100
                logger.info(
                    f"Cache stats node {self.node_id}: "
                    f"size={len(self._cache)}, "
                    f"hit_rate={hit_rate:.1f}%, "
                    f"hits={self._hit_count}, misses={self._miss_count}"
                )

    def get_status(self) -> dict:
        total = self._hit_count + self._miss_count
        hit_rate = self._hit_count / total if total > 0 else 0
        return {
            "node_id": self.node_id,
            "protocol": config.cache.protocol,
            "replacement_policy": config.cache.replacement_policy,
            "size": len(self._cache),
            "max_size": config.cache.max_size,
            "hit_count": self._hit_count,
            "miss_count": self._miss_count,
            "hit_rate": round(hit_rate, 4),
        }