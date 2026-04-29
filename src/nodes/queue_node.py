import asyncio
import hashlib
import json
import logging
import time
import uuid
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Set

import redis.asyncio as aioredis

from src.utils.config import config
from src.utils.metrics import metrics

logger = logging.getLogger(__name__)


@dataclass
class Message:
    # Satu pesan dalam antrian; delivery_count naik setiap kali dikirim ulang
    message_id: str
    queue_name: str
    payload: dict
    producer_id: str
    produced_at: float
    delivery_count: int = 0
    ack_deadline: Optional[float] = None  # Diisi saat pesan sedang in-flight

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "Message":
        return cls(**d)


@dataclass
class ProduceRequest:
    queue_name: str
    payload: dict
    producer_id: str


@dataclass
class ConsumeRequest:
    queue_name: str
    consumer_id: str
    ack_timeout: int = 30  # Detik sebelum pesan dikirim ulang jika tidak di-ack


@dataclass
class ConsumeResult:
    success: bool
    message: Optional[Message] = None
    error: str = ""


class ConsistentHashRing:
    """
    Hash ring untuk mendistribusikan queue ke node.
    Setiap node direpresentasikan oleh beberapa virtual node (replicas)
    agar distribusi lebih merata.
    """
    def __init__(self, nodes: List[int], replicas: int = 100):
        self._ring: Dict[int, int] = {}
        self._sorted_keys: List[int] = []
        self.replicas = replicas

        for node_id in nodes:
            self.add_node(node_id)

    def add_node(self, node_id: int):
        # Tambahkan virtual nodes ke ring lalu urutkan ulang
        for i in range(self.replicas):
            key = self._hash(f"{node_id}:{i}")
            self._ring[key] = node_id
        self._sorted_keys = sorted(self._ring.keys())

    def remove_node(self, node_id: int):
        # Hapus semua virtual nodes milik node ini
        for i in range(self.replicas):
            key = self._hash(f"{node_id}:{i}")
            self._ring.pop(key, None)
        self._sorted_keys = sorted(self._ring.keys())

    def get_node(self, key: str) -> Optional[int]:
        # Cari node pertama di ring yang hash-nya >= hash(key); wrap jika perlu
        if not self._ring:
            return None
        hashed = self._hash(key)
        for ring_key in self._sorted_keys:
            if hashed <= ring_key:
                return self._ring[ring_key]
        return self._ring[self._sorted_keys[0]]

    def get_replica_nodes(self, key: str, count: int) -> List[int]:
        # Ambil `count` node unik berurutan dari posisi hash(key) di ring
        if not self._ring:
            return []
        hashed = self._hash(key)
        seen: Set[int] = set()
        result: List[int] = []

        start_idx = 0
        for i, ring_key in enumerate(self._sorted_keys):
            if hashed <= ring_key:
                start_idx = i
                break

        # Iterasi melingkar dari start_idx
        indices = list(range(start_idx, len(self._sorted_keys))) + \
                  list(range(0, start_idx))

        for idx in indices:
            node_id = self._ring[self._sorted_keys[idx]]
            if node_id not in seen:
                seen.add(node_id)
                result.append(node_id)
                if len(result) >= count:
                    break

        return result

    @staticmethod
    def _hash(key: str) -> int:
        return int(hashlib.md5(key.encode()).hexdigest(), 16)


class QueueNode:
    """
    Node antrian terdistribusi berbasis Raft.
    Produce/ack direplikasi via Raft log; consume dilayani langsung dari memori lokal.
    Redis dipakai untuk persistensi agar pesan tidak hilang saat restart.
    """
    def __init__(self, raft_node):
        self.raft = raft_node
        self.node_id = config.node.node_id

        all_node_ids = list(range(1, config.cluster.cluster_size + 1))
        self._hash_ring = ConsistentHashRing(all_node_ids)

        self._local_queues: Dict[str, List[Message]] = {}  # Queue in-memory per nama
        self._in_flight: Dict[str, Message] = {}           # Pesan yang sudah consume tapi belum di-ack
        self._redis: Optional[aioredis.Redis] = None
        self._running = False
        self._tasks: List[asyncio.Task] = []

        self.raft.register_state_machine(self._apply_command)

    async def start(self):
        # Buka koneksi Redis, pulihkan state, lalu jalankan background tasks
        self._redis = await aioredis.from_url(
            config.redis.url,
            max_connections=config.redis.max_connections,
            decode_responses=True,
        )
        self._running = True
        self._tasks = [
            asyncio.create_task(self._redelivery_loop()),
            asyncio.create_task(self._metrics_loop()),
        ]
        await self._restore_from_redis()
        logger.info(f"QueueNode {self.node_id} started")

    async def stop(self):
        self._running = False
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        if self._redis:
            await self._redis.aclose()

    def is_responsible_for(self, queue_name: str) -> bool:
        # Node ini bertanggung jawab jika masuk dalam daftar replica node untuk queue ini
        replicas = self._hash_ring.get_replica_nodes(
            queue_name, config.queue.replication_factor if hasattr(config, 'queue') else 2
        )
        return self.node_id in replicas

    async def produce(self, request: ProduceRequest) -> dict:
        # Buat message_id, submit ke Raft log, tunggu commit
        message_id = str(uuid.uuid4())
        command = {
            "op": "QUEUE_PRODUCE",
            "message_id": message_id,
            "queue_name": request.queue_name,
            "payload": request.payload,
            "producer_id": request.producer_id,
            "produced_at": time.time(),
        }

        if not self.raft.is_leader():
            return {"success": False, "error": "Bukan leader"}

        log_index = await self.raft.submit_command(command)
        if log_index is None:
            return {"success": False, "error": "Gagal submit ke Raft"}

        committed = await self.raft.wait_for_commit(log_index, timeout=5.0)
        if not committed:
            return {"success": False, "error": "Commit timeout"}

        metrics.queue_messages_produced.labels(
            node_id=str(self.node_id),
            queue_name=request.queue_name
        ).inc()

        return {"success": True, "message_id": message_id}

    async def consume(self, request: ConsumeRequest) -> ConsumeResult:
        # Ambil pesan pertama dari queue lokal dan pindahkan ke in-flight
        queue = self._local_queues.get(request.queue_name, [])
        if not queue:
            return ConsumeResult(success=False, error="Queue kosong")

        message = queue.pop(0)
        message.delivery_count += 1
        message.ack_deadline = time.time() + request.ack_timeout

        self._in_flight[message.message_id] = message

        metrics.queue_messages_consumed.labels(
            node_id=str(self.node_id),
            queue_name=request.queue_name,
        ).inc()

        return ConsumeResult(success=True, message=message)

    async def acknowledge(self, message_id: str, consumer_id: str) -> bool:
        # Hapus dari in-flight, replikasi ACK ke Raft, bersihkan Redis
        if message_id not in self._in_flight:
            return False

        message = self._in_flight.pop(message_id)
        command = {
            "op": "QUEUE_ACK",
            "message_id": message_id,
            "queue_name": message.queue_name,
            "consumer_id": consumer_id,
        }

        if self.raft.is_leader():
            await self.raft.submit_command(command)

        await self._delete_message_from_redis(message.queue_name, message_id)
        return True

    async def _apply_command(self, command: dict):
        # Dipanggil Raft setelah commit; dijalankan di semua node
        op = command.get("op")

        if op == "QUEUE_PRODUCE":
            message = Message(
                message_id=command["message_id"],
                queue_name=command["queue_name"],
                payload=command["payload"],
                producer_id=command["producer_id"],
                produced_at=command["produced_at"],
            )
            if command["queue_name"] not in self._local_queues:
                self._local_queues[command["queue_name"]] = []
            self._local_queues[command["queue_name"]].append(message)
            await self._persist_message(message)

        elif op == "QUEUE_ACK":
            # Hapus pesan dari queue lokal berdasarkan message_id
            queue_name = command["queue_name"]
            if queue_name in self._local_queues:
                self._local_queues[queue_name] = [
                    m for m in self._local_queues[queue_name]
                    if m.message_id != command["message_id"]
                ]

    async def _persist_message(self, message: Message):
        if not self._redis:
            return
        key = f"queue:{message.queue_name}:{message.message_id}"
        await self._redis.set(key, json.dumps(message.to_dict()))

    async def _delete_message_from_redis(self, queue_name: str, message_id: str):
        if not self._redis:
            return
        await self._redis.delete(f"queue:{queue_name}:{message_id}")

    async def _restore_from_redis(self):
        # Muat ulang semua pesan dari Redis ke memori saat node start
        if not self._redis:
            return
        try:
            keys = await self._redis.keys("queue:*")
            restored = 0
            for key in keys:
                raw = await self._redis.get(key)
                if raw:
                    message = Message.from_dict(json.loads(raw))
                    if message.queue_name not in self._local_queues:
                        self._local_queues[message.queue_name] = []
                    self._local_queues[message.queue_name].append(message)
                    restored += 1
            logger.info(f"QueueNode restored {restored} messages dari Redis")
        except Exception as e:
            logger.error(f"Gagal restore dari Redis: {e}")

    async def _redelivery_loop(self):
        # Setiap 5 detik, kembalikan pesan in-flight yang melewati ack_deadline ke depan queue
        while self._running:
            await asyncio.sleep(5.0)
            now = time.time()
            requeue = [
                msg for mid, msg in list(self._in_flight.items())
                if msg.ack_deadline and now > msg.ack_deadline
            ]
            for message in requeue:
                del self._in_flight[message.message_id]
                logger.warning(
                    f"Redelivery message {message.message_id} "
                    f"(delivery #{message.delivery_count})"
                )
                self._local_queues[message.queue_name].insert(0, message)

    async def _metrics_loop(self):
        # Perbarui metrik kedalaman queue setiap 10 detik
        while self._running:
            await asyncio.sleep(10.0)
            for queue_name, messages in self._local_queues.items():
                metrics.queue_depth.labels(
                    node_id=str(self.node_id),
                    queue_name=queue_name,
                ).set(len(messages))

    def get_status(self) -> dict:
        # Snapshot status node: kedalaman queue, jumlah in-flight, dan queue yang ditangani node ini
        return {
            "node_id": self.node_id,
            "queues": {
                name: {
                    "depth": len(messages),
                    "in_flight": sum(
                        1 for m in self._in_flight.values()
                        if m.queue_name == name
                    ),
                }
                for name, messages in self._local_queues.items()
            },
            "hash_ring_responsible": [
                q for q in self._local_queues if self.is_responsible_for(q)
            ],
        }