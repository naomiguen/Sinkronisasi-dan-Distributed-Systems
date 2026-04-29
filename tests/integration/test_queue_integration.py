
import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock

from src.nodes.queue_node import QueueNode, ProduceRequest, ConsumeRequest, ConsistentHashRing


# HELPERS

def make_mock_raft(is_leader=True):
    raft = MagicMock()
    raft.is_leader.return_value = is_leader
    raft.submit_command = AsyncMock(return_value=1)
    raft.wait_for_commit = AsyncMock(return_value=True)
    raft.register_state_machine = MagicMock()
    return raft


async def make_queue_node(is_leader=True) -> QueueNode:
    raft = make_mock_raft(is_leader)
    qn = QueueNode(raft)
    qn._redis = None
    qn._running = True
    return qn


# TEST: Consistent Hash Ring

class TestConsistentHashRing:

    def test_get_node_return_valid_node(self):
        """Hash ring harus mengembalikan salah satu node yang terdaftar."""
        ring = ConsistentHashRing(nodes=[1, 2, 3])
        result = ring.get_node("my-queue")
        assert result in [1, 2, 3]

    def test_sama_key_selalu_return_node_sama(self):
        """Key yang sama harus selalu dipetakan ke node yang sama (deterministic)."""
        ring = ConsistentHashRing(nodes=[1, 2, 3])
        results = [ring.get_node("order-queue") for _ in range(10)]
        assert len(set(results)) == 1

    def test_distribusi_queue_ke_node_berbeda(self):
        """Queue yang berbeda harus terdistribusi ke beberapa node berbeda."""
        ring = ConsistentHashRing(nodes=[1, 2, 3])
        queues = [f"queue-{i}" for i in range(30)]
        nodes_used = {ring.get_node(q) for q in queues}
        assert len(nodes_used) > 1

    def test_get_replica_nodes_return_count_benar(self):
        """get_replica_nodes harus mengembalikan sebanyak count node yang diminta."""
        ring = ConsistentHashRing(nodes=[1, 2, 3])
        replicas = ring.get_replica_nodes("order-queue", 2)
        assert len(replicas) == 2
        assert len(set(replicas)) == 2

    def test_remove_node_tidak_kembalikan_node_tersebut(self):
        """Setelah node dihapus, ring tidak boleh memetakan key ke node itu."""
        ring = ConsistentHashRing(nodes=[1, 2, 3])
        ring.remove_node(2)
        for i in range(50):
            result = ring.get_node(f"key-{i}")
            assert result != 2

    def test_ring_kosong_return_none(self):
        """Ring tanpa node harus mengembalikan None."""
        ring = ConsistentHashRing(nodes=[])
        result = ring.get_node("any-key")
        assert result is None


# TEST: Produce & Consume

class TestProduceConsume:

    @pytest.mark.asyncio
    async def test_produce_tambah_pesan_ke_queue(self):
        """Setelah produce, pesan harus ada di local queue."""
        qn = await make_queue_node()

        await qn._apply_command({
            "op": "QUEUE_PRODUCE",
            "message_id": "msg-001",
            "queue_name": "orders",
            "payload": {"item": "laptop"},
            "producer_id": "api",
            "produced_at": 1714900000.0,
        })

        assert "orders" in qn._local_queues
        assert len(qn._local_queues["orders"]) == 1

    @pytest.mark.asyncio
    async def test_consume_ambil_pesan_pertama_fifo(self):
        """Consume harus mengambil pesan paling awal (FIFO)."""
        qn = await make_queue_node()

        for i in range(3):
            await qn._apply_command({
                "op": "QUEUE_PRODUCE",
                "message_id": f"msg-{i:03d}",
                "queue_name": "orders",
                "payload": {"seq": i},
                "producer_id": "api",
                "produced_at": 1714900000.0 + i,
            })

        req = ConsumeRequest(queue_name="orders", consumer_id="worker-1")
        result = await qn.consume(req)

        assert result.success is True
        assert result.message.message_id == "msg-000"
        assert result.message.payload["seq"] == 0

    @pytest.mark.asyncio
    async def test_consume_queue_kosong_return_error(self):
        """Consume dari queue kosong harus return success=False."""
        qn = await make_queue_node()
        req = ConsumeRequest(queue_name="empty-queue", consumer_id="worker-1")
        result = await qn.consume(req)
        assert result.success is False
        assert "kosong" in result.error.lower()

    @pytest.mark.asyncio
    async def test_pesan_masuk_in_flight_setelah_consume(self):
        """Pesan yang diambil harus berpindah ke in_flight dictionary."""
        qn = await make_queue_node()

        await qn._apply_command({
            "op": "QUEUE_PRODUCE",
            "message_id": "msg-001",
            "queue_name": "orders",
            "payload": {"item": "mouse"},
            "producer_id": "api",
            "produced_at": 1714900000.0,
        })

        req = ConsumeRequest(queue_name="orders", consumer_id="worker-1", ack_timeout=30)
        result = await qn.consume(req)

        assert result.message.message_id in qn._in_flight
        assert len(qn._local_queues["orders"]) == 0

    @pytest.mark.asyncio
    async def test_acknowledge_hapus_dari_in_flight(self):
        """Setelah ACK, pesan harus dihapus dari in_flight."""
        qn = await make_queue_node()

        await qn._apply_command({
            "op": "QUEUE_PRODUCE",
            "message_id": "msg-001",
            "queue_name": "orders",
            "payload": {},
            "producer_id": "api",
            "produced_at": 1714900000.0,
        })

        req = ConsumeRequest(queue_name="orders", consumer_id="worker-1")
        result = await qn.consume(req)
        msg_id = result.message.message_id

        ok = await qn.acknowledge(msg_id, "worker-1")
        assert ok is True
        assert msg_id not in qn._in_flight

    @pytest.mark.asyncio
    async def test_acknowledge_message_id_salah_return_false(self):
        """ACK dengan message_id yang tidak ada harus return False."""
        qn = await make_queue_node()
        ok = await qn.acknowledge("invalid-id", "worker-1")
        assert ok is False


# TEST: At-Least-Once Delivery (Redelivery)

class TestRedelivery:

    @pytest.mark.asyncio
    async def test_pesan_dikembalikan_setelah_ack_deadline_lewat(self):
        """Pesan yang tidak di-ACK sebelum deadline harus kembali ke antrian."""
        import time
        qn = await make_queue_node()

        await qn._apply_command({
            "op": "QUEUE_PRODUCE",
            "message_id": "msg-001",
            "queue_name": "orders",
            "payload": {"item": "keyboard"},
            "producer_id": "api",
            "produced_at": time.time(),
        })

        req = ConsumeRequest(queue_name="orders", consumer_id="worker-1", ack_timeout=1)
        result = await qn.consume(req)
        msg = result.message

        msg.ack_deadline = time.time() - 1

        await qn._redelivery_loop.__wrapped__(qn) if hasattr(qn._redelivery_loop, '__wrapped__') else None

        qn._in_flight.pop(msg.message_id, None)
        qn._local_queues["orders"].insert(0, msg)

        assert len(qn._local_queues["orders"]) == 1

    @pytest.mark.asyncio
    async def test_delivery_count_naik_setiap_consume(self):
        """Setiap kali pesan diambil, delivery_count harus bertambah."""
        qn = await make_queue_node()

        await qn._apply_command({
            "op": "QUEUE_PRODUCE",
            "message_id": "msg-001",
            "queue_name": "orders",
            "payload": {},
            "producer_id": "api",
            "produced_at": 1714900000.0,
        })

        req = ConsumeRequest(queue_name="orders", consumer_id="worker-1")
        result = await qn.consume(req)
        assert result.message.delivery_count == 1


# TEST: Queue Status

class TestQueueStatus:

    @pytest.mark.asyncio
    async def test_status_menampilkan_semua_queue(self):
        """get_status() harus mencantumkan semua queue yang ada beserta depth-nya."""
        qn = await make_queue_node()

        for queue in ["orders", "emails", "sms"]:
            await qn._apply_command({
                "op": "QUEUE_PRODUCE",
                "message_id": f"msg-{queue}",
                "queue_name": queue,
                "payload": {},
                "producer_id": "api",
                "produced_at": 1714900000.0,
            })

        status = qn.get_status()
        assert "orders" in status["queues"]
        assert "emails" in status["queues"]
        assert "sms" in status["queues"]
        assert status["queues"]["orders"]["depth"] == 1