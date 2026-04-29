import asyncio
import time
import logging
from enum import Enum
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Set
from collections import defaultdict

import redis.asyncio as aioredis

from src.utils.config import config
from src.utils.metrics import metrics

logger = logging.getLogger(__name__)


class LockType(Enum):
    SHARED = "shared"       # Bisa dipegang banyak client (read lock)
    EXCLUSIVE = "exclusive" # Hanya satu client (write lock)


class LockStatus(Enum):
    GRANTED = "granted"   # Lock berhasil diperoleh
    DENIED = "denied"     # Lock ditolak
    TIMEOUT = "timeout"   # Waktu tunggu habis
    RELEASED = "released" # Lock dilepas


@dataclass
class LockInfo:
    # Metadata lock yang sedang aktif, expires_at dihitung otomatis
    resource: str
    lock_type: LockType
    client_id: str
    node_id: int
    acquired_at: float
    ttl: int
    expires_at: float = field(init=False)

    def __post_init__(self):
        self.expires_at = self.acquired_at + self.ttl

    def is_expired(self) -> bool:
        return time.time() > self.expires_at

    def to_dict(self) -> dict:
        # Dipakai saat menyimpan ke Redis
        return {
            "resource": self.resource,
            "lock_type": self.lock_type.value,
            "client_id": self.client_id,
            "node_id": self.node_id,
            "acquired_at": self.acquired_at,
            "ttl": self.ttl,
            "expires_at": self.expires_at,
        }


@dataclass
class LockRequest:
    # Payload permintaan lock dari client
    resource: str
    lock_type: LockType
    client_id: str
    ttl: int = field(default_factory=lambda: config.lock.default_ttl)


@dataclass
class LockResult:
    # Hasil operasi acquire/release yang dikembalikan ke client
    status: LockStatus
    resource: str
    client_id: str
    message: str = ""
    lock_info: Optional[LockInfo] = None


class WaitQueue:
    # Antrian tunggu per resource berbasis asyncio.Queue, menghindari busy-waiting
    def __init__(self):
        self._queues: Dict[str, asyncio.Queue] = defaultdict(asyncio.Queue)

    async def wait(self, resource: str, timeout: float) -> bool:
        # Tunggu notifikasi; return False jika timeout
        q = self._queues[resource]
        try:
            await asyncio.wait_for(q.get(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            return False

    async def notify(self, resource: str):
        # Kirim sinyal ke waiter; skip jika sudah ada notifikasi pending
        q = self._queues[resource]
        if not q.empty():
            return
        await q.put(True)

    def enqueue(self, resource: str):
        # Pastikan entry queue untuk resource ini sudah ada
        self._queues[resource]


class DeadlockDetector:
    # Deteksi deadlock via graf wait-for; edge A→B artinya A menunggu B
    def __init__(self):
        self._wait_for: Dict[str, Set[str]] = defaultdict(set)

    def add_wait(self, waiter_client: str, holder_clients: Set[str]):
        self._wait_for[waiter_client] = holder_clients.copy()

    def remove_wait(self, client_id: str):
        self._wait_for.pop(client_id, None)

    def has_cycle(self) -> Optional[List[str]]:
        # DFS untuk menemukan siklus; kembalikan list client pembentuk siklus
        visited: Set[str] = set()
        path: List[str] = []

        def dfs(node: str) -> Optional[List[str]]:
            if node in path:
                return path[path.index(node):]  # Siklus ditemukan
            if node in visited:
                return None
            visited.add(node)
            path.append(node)
            for neighbor in self._wait_for.get(node, set()):
                result = dfs(neighbor)
                if result:
                    return result
            path.pop()
            return None

        for client in list(self._wait_for.keys()):
            if client not in visited:
                cycle = dfs(client)
                if cycle:
                    return cycle
        return None

    def detect_and_report(self) -> Optional[List[str]]:
        # Deteksi siklus, catat ke metrics dan log jika ditemukan
        cycle = self.has_cycle()
        if cycle:
            metrics.deadlock_detected.labels(node_id=str(config.node.node_id)).inc()
            logger.warning(f"Deadlock terdeteksi: {' -> '.join(cycle)}")
        return cycle


class LockManager:
    """
    Manajer lock terdistribusi berbasis Raft consensus.
    Hanya leader yang memproses acquire/release; Redis dipakai untuk persistensi.
    """
    def __init__(self, raft_node):
        self.raft = raft_node
        self.node_id = config.node.node_id

        self._locks: Dict[str, List[LockInfo]] = defaultdict(list)
        self._wait_queue = WaitQueue()
        self._deadlock_detector = DeadlockDetector()
        self._redis: Optional[aioredis.Redis] = None
        self._running = False
        self._tasks: List[asyncio.Task] = []

        # Daftarkan sebagai state machine Raft agar setiap commit dieksekusi di sini
        self.raft.register_state_machine(self.apply_command)

    async def start(self):
        # Buka koneksi Redis dan jalankan background tasks
        self._redis = await aioredis.from_url(
            config.redis.url,
            max_connections=config.redis.max_connections,
            decode_responses=True,
        )
        self._running = True
        self._tasks = [
            asyncio.create_task(self._ttl_expiry_loop()),
            asyncio.create_task(self._deadlock_check_loop()),
            asyncio.create_task(self._sync_from_redis()),
        ]
        logger.info(f"LockManager node {self.node_id} started")

    async def stop(self):
        # Batalkan semua task dan tutup koneksi Redis
        self._running = False
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        if self._redis:
            await self._redis.aclose()

    async def acquire(self, request: LockRequest) -> LockResult:
        """
        Alur: cek leader → cek _can_acquire → submit Raft → tunggu commit.
        Jika lock belum tersedia, masuk antrian tunggu sambil pantau deadlock.
        """
        if not self.raft.is_leader():
            return LockResult(
                status=LockStatus.DENIED,
                resource=request.resource,
                client_id=request.client_id,
                message="Bukan leader, redirect ke leader",
            )

        ttl = min(request.ttl, config.lock.max_ttl)  # Batasi TTL sesuai konfigurasi
        command = {
            "op": "LOCK_ACQUIRE",
            "resource": request.resource,
            "lock_type": request.lock_type.value,
            "client_id": request.client_id,
            "node_id": self.node_id,
            "ttl": ttl,
        }

        start = time.time()

        for attempt in range(config.lock.max_retries):
            if self._can_acquire(request.resource, request.lock_type, request.client_id):
                log_index = await self.raft.submit_command(command)
                if log_index is None:
                    return LockResult(
                        status=LockStatus.DENIED,
                        resource=request.resource,
                        client_id=request.client_id,
                        message="Gagal submit ke Raft log",
                    )

                # Tunggu mayoritas node mengakui entry ini
                committed = await self.raft.wait_for_commit(log_index, timeout=5.0)
                if not committed:
                    return LockResult(
                        status=LockStatus.TIMEOUT,
                        resource=request.resource,
                        client_id=request.client_id,
                        message="Raft commit timeout",
                    )

                lock_info = self._get_lock(request.resource, request.client_id)
                metrics.record_lock_latency(time.time() - start, "acquire")
                metrics.lock_granted(request.lock_type.value)

                return LockResult(
                    status=LockStatus.GRANTED,
                    resource=request.resource,
                    client_id=request.client_id,
                    message="Lock berhasil diperoleh",
                    lock_info=lock_info,
                )

            # Lock belum tersedia → pantau deadlock sebelum menunggu
            holders = self._get_holders(request.resource)
            self._deadlock_detector.add_wait(request.client_id, holders)

            cycle = self._deadlock_detector.detect_and_report()
            if cycle:
                # Batalkan untuk memutus siklus deadlock
                self._deadlock_detector.remove_wait(request.client_id)
                return LockResult(
                    status=LockStatus.DENIED,
                    resource=request.resource,
                    client_id=request.client_id,
                    message=f"Deadlock terdeteksi: {' -> '.join(cycle)}",
                )

            await self._wait_queue.wait(
                request.resource,
                timeout=config.lock.retry_interval / 1000.0,
            )

        # Semua percobaan habis
        self._deadlock_detector.remove_wait(request.client_id)
        metrics.lock_denied(request.lock_type.value)

        return LockResult(
            status=LockStatus.TIMEOUT,
            resource=request.resource,
            client_id=request.client_id,
            message=f"Lock timeout setelah {config.lock.max_retries} percobaan",
        )

    async def release(self, resource: str, client_id: str) -> LockResult:
        # Submit LOCK_RELEASE ke Raft log; hanya leader yang boleh memproses
        if not self.raft.is_leader():
            return LockResult(
                status=LockStatus.DENIED,
                resource=resource,
                client_id=client_id,
                message="Bukan leader",
            )

        command = {
            "op": "LOCK_RELEASE",
            "resource": resource,
            "client_id": client_id,
        }

        log_index = await self.raft.submit_command(command)
        if log_index is None:
            return LockResult(
                status=LockStatus.DENIED,
                resource=resource,
                client_id=client_id,
                message="Gagal submit ke Raft log",
            )

        await self.raft.wait_for_commit(log_index, timeout=5.0)
        metrics.record_lock_latency(0, "release")

        return LockResult(
            status=LockStatus.RELEASED,
            resource=resource,
            client_id=client_id,
            message="Lock berhasil dilepas",
        )

    async def apply_command(self, command: dict):
        # Dipanggil Raft setelah commit; dijalankan di semua node untuk konsistensi
        op = command.get("op")

        if op == "LOCK_ACQUIRE":
            lock_info = LockInfo(
                resource=command["resource"],
                lock_type=LockType(command["lock_type"]),
                client_id=command["client_id"],
                node_id=command["node_id"],
                acquired_at=time.time(),
                ttl=command["ttl"],
            )
            self._locks[command["resource"]].append(lock_info)
            await self._persist_lock(lock_info)

            count = sum(len(v) for v in self._locks.values())
            metrics.active_locks.labels(node_id=str(self.node_id)).set(count)

        elif op == "LOCK_RELEASE":
            resource = command["resource"]
            client_id = command["client_id"]
            self._locks[resource] = [
                l for l in self._locks[resource] if l.client_id != client_id
            ]
            await self._delete_lock(resource, client_id)
            await self._wait_queue.notify(resource)  # Bangunkan waiter
            self._deadlock_detector.remove_wait(client_id)

            count = sum(len(v) for v in self._locks.values())
            metrics.active_locks.labels(node_id=str(self.node_id)).set(count)

    def _can_acquire(self, resource: str, lock_type: LockType, client_id: str) -> bool:
        # Shared: boleh jika semua pemegang juga shared; Exclusive: boleh jika hanya milik sendiri
        existing = [l for l in self._locks[resource] if not l.is_expired()]

        if not existing:
            return True
        if lock_type == LockType.SHARED:
            return all(l.lock_type == LockType.SHARED for l in existing)
        if lock_type == LockType.EXCLUSIVE:
            return all(l.client_id == client_id for l in existing)
        return False

    def _get_holders(self, resource: str) -> Set[str]:
        return {l.client_id for l in self._locks[resource] if not l.is_expired()}

    def _get_lock(self, resource: str, client_id: str) -> Optional[LockInfo]:
        for lock in self._locks[resource]:
            if lock.client_id == client_id:
                return lock
        return None

    def get_all_locks(self) -> dict:
        # Kembalikan semua lock aktif (belum expired) dalam bentuk dict
        result = {}
        for resource, locks in self._locks.items():
            active = [l.to_dict() for l in locks if not l.is_expired()]
            if active:
                result[resource] = active
        return result

    async def _persist_lock(self, lock_info: LockInfo):
        # Simpan lock ke Redis dengan expiry otomatis sesuai TTL
        if not self._redis:
            return
        key = f"lock:{lock_info.resource}:{lock_info.client_id}"
        await self._redis.hset(key, mapping=lock_info.to_dict())
        await self._redis.expireat(key, int(lock_info.expires_at))

    async def _delete_lock(self, resource: str, client_id: str):
        if not self._redis:
            return
        await self._redis.delete(f"lock:{resource}:{client_id}")

    async def _ttl_expiry_loop(self):
        # Cek dan lepas lock yang sudah expired setiap 1 detik
        while self._running:
            await asyncio.sleep(1.0)
            expired = [
                (res, lock.client_id)
                for res, locks in self._locks.items()
                for lock in locks
                if lock.is_expired()
            ]
            for resource, client_id in expired:
                logger.info(f"Lock expired: resource={resource}, client={client_id}")
                await self.release(resource, client_id)

    async def _deadlock_check_loop(self):
        # Cek deadlock secara periodik setiap 5 detik
        while self._running:
            await asyncio.sleep(5.0)
            self._deadlock_detector.detect_and_report()

    async def _sync_from_redis(self):
        # Pulihkan state lock dari Redis saat node baru start/restart
        if not self._redis:
            return
        await asyncio.sleep(2.0)

        try:
            keys = await self._redis.keys("lock:*")
            for key in keys:
                data = await self._redis.hgetall(key)
                if data:
                    lock = LockInfo(
                        resource=data["resource"],
                        lock_type=LockType(data["lock_type"]),
                        client_id=data["client_id"],
                        node_id=int(data["node_id"]),
                        acquired_at=float(data["acquired_at"]),
                        ttl=int(data["ttl"]),
                    )
                    if not lock.is_expired():
                        self._locks[lock.resource].append(lock)
            logger.info(f"Sync dari Redis selesai: {len(keys)} lock dipulihkan")
        except Exception as e:
            logger.error(f"Gagal sync dari Redis: {e}")