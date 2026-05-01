import asyncio
import logging
import sys
from typing import Optional

from aiohttp import web

from src.utils.config import config
from src.utils.metrics import metrics
from src.consensus.raft import RaftNode
from src.communication.message_passing import MessagePassing
from src.communication.failure_detector import FailureDetector
from src.nodes.lock_manager import LockManager, LockRequest, LockType
from src.nodes.queue_node import QueueNode, ProduceRequest, ConsumeRequest
from src.nodes.cache_node import CacheNode

logging.basicConfig(
    level=getattr(logging, config.log_level.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


class Node:
    def __init__(self):
        self.node_id = config.node.node_id
        self.host = config.node.host
        self.port = config.node.port

        cluster_size = config.cluster.cluster_size
        self.peer_ids = [i for i in range(1, cluster_size + 1) if i != self.node_id]

        self.raft = RaftNode(node_id=self.node_id, peers=self.peer_ids)
        self.message_passing = MessagePassing(self.raft)
        self.failure_detector = FailureDetector(self.node_id, self.peer_ids)
        self.lock_manager = LockManager(self.raft)
        self.queue_node = QueueNode(self.raft)
        self.cache_node = CacheNode(self.raft)

        self.app = web.Application()
        self._setup_routes()
        self.runner: Optional[web.AppRunner] = None

    def _setup_routes(self):
        routes = [
            web.get("/health", self.handle_health),
            web.get("/status", self.handle_status),
            web.get("/metrics", self.handle_metrics),
        ]
        routes += self.message_passing.get_routes()
        routes += [
            web.post("/lock/acquire",             self.handle_lock_acquire),
            web.post("/lock/release",             self.handle_lock_release),
            web.get("/lock/status",               self.handle_lock_status),
            web.post("/queue/produce",            self.handle_queue_produce),
            web.post("/queue/consume",            self.handle_queue_consume),
            web.post("/queue/ack",                self.handle_queue_ack),
            web.get("/queue/status",              self.handle_queue_status),
            web.put("/cache/set",                 self.handle_cache_set),
            web.get("/cache/get/{key}",           self.handle_cache_get),
            web.delete("/cache/invalidate/{key}", self.handle_cache_invalidate),
            web.get("/cache/status",              self.handle_cache_status),
        ]
        self.app.add_routes(routes)

    # ── LIFECYCLE ──────────────────────────────────────────
    async def start(self):
        logger.info(f"Node {self.node_id} starting...")
        if config.metrics_enabled:
            metrics.start_server()
        await self.message_passing.start()
        await self.failure_detector.start()
        self.failure_detector.on_node_dead(self._on_peer_dead)
        self.failure_detector.on_node_recovered(self._on_peer_recovered)
        await self.raft.start()
        await self.lock_manager.start()
        await self.queue_node.start()
        await self.cache_node.start()
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, self.host, self.port)
        await site.start()
        logger.info(f"Node {self.node_id} ONLINE di http://{self.host}:{self.port}")

    async def stop(self):
        logger.info(f"Node {self.node_id} shutting down...")
        await self.cache_node.stop()
        await self.queue_node.stop()
        await self.lock_manager.stop()
        await self.raft.stop()
        await self.failure_detector.stop()
        await self.message_passing.stop()
        if self.runner:
            await self.runner.cleanup()

    async def _on_peer_dead(self, peer_id: int):
        logger.warning(f"Peer {peer_id} DEAD")

    async def _on_peer_recovered(self, peer_id: int):
        logger.info(f"Peer {peer_id} RECOVERED")

    # ── HELPER ─────────────────────────────────────────────
    def _leader_only(self) -> Optional[web.Response]:
        if self.raft.is_leader():
            return None
        leader_id = self.raft.get_leader_id()
        if leader_id is None:
            return web.json_response({"error": "Tidak ada leader"}, status=503)
        nodes = config.cluster.nodes
        if leader_id <= len(nodes):
            n = nodes[leader_id - 1]
            return web.json_response({
                "error": "Bukan leader",
                "leader_id": leader_id,
                "leader_url": f"http://{n['host']}:{n['port']}",
            }, status=307)
        return web.json_response({"error": "Leader tidak diketahui"}, status=503)

    # ── HEALTH & STATUS ────────────────────────────────────
    async def handle_health(self, request: web.Request) -> web.Response:
        return web.json_response({
            "status": "ok",
            "node_id": self.node_id,
            "role": self.raft.role.value,
        })

    async def handle_status(self, request: web.Request) -> web.Response:
        return web.json_response({
            "raft": self.raft.get_status(),
            "peers": self.failure_detector.get_status_all(),
            "config": {
                "node_id": self.node_id,
                "port": self.port,
                "cluster_size": config.cluster.cluster_size,
                "quorum": config.cluster.quorum,
            }
        })

    async def handle_metrics(self, request: web.Request) -> web.Response:
        return web.json_response({
            "node_id": self.node_id,
            "role": self.raft.role.value,
            "locks": self.lock_manager.get_all_locks(),
            "queue": self.queue_node.get_status(),
            "cache": self.cache_node.get_status(),
        })

    # ── LOCK ───────────────────────────────────────────────
    async def handle_lock_acquire(self, request: web.Request) -> web.Response:
        if r := self._leader_only():
            return r
        try:
            data = await request.json()
            result = await self.lock_manager.acquire(LockRequest(
                resource=data["resource"],
                lock_type=LockType(data.get("lock_type", "exclusive")),
                client_id=data["client_id"],
                ttl=data.get("ttl", config.lock.default_ttl),
            ))
            return web.json_response({
                "status": result.status.value,
                "resource": result.resource,
                "client_id": result.client_id,
                "message": result.message,
                "lock_info": result.lock_info.to_dict() if result.lock_info else None,
            })
        except Exception as e:
            logger.error(f"lock_acquire error: {e}")
            return web.json_response({"error": str(e)}, status=400)

    async def handle_lock_release(self, request: web.Request) -> web.Response:
        if r := self._leader_only():
            return r
        try:
            data = await request.json()
            result = await self.lock_manager.release(data["resource"], data["client_id"])
            return web.json_response({
                "status": result.status.value,
                "message": result.message,
            })
        except Exception as e:
            logger.error(f"lock_release error: {e}")
            return web.json_response({"error": str(e)}, status=400)

    async def handle_lock_status(self, request: web.Request) -> web.Response:
        return web.json_response({"locks": self.lock_manager.get_all_locks()})

    # ── QUEUE ──────────────────────────────────────────────
    async def handle_queue_produce(self, request: web.Request) -> web.Response:
        if r := self._leader_only():
            return r
        try:
            data = await request.json()
            # Terima baik "payload" maupun "message" sebagai field nama pesan
            payload = data.get("payload") or data.get("message") or ""
            result = await self.queue_node.produce(ProduceRequest(
                queue_name=data["queue"],
                payload=payload,
                producer_id=data.get("producer_id", "anonymous"),
            ))
            return web.json_response(result)
        except Exception as e:
            logger.error(f"queue_produce error: {e}")
            return web.json_response({"error": str(e)}, status=400)

    async def handle_queue_consume(self, request: web.Request) -> web.Response:
        try:
            data = await request.json()
            result = await self.queue_node.consume(ConsumeRequest(
                queue_name=data["queue"],
                consumer_id=data.get("consumer_id", "default"),
                ack_timeout=data.get("ack_timeout", 30),
            ))
            if result.success and result.message is not None:
                return web.json_response({
                    "success": True,
                    "message": result.message.to_dict(),
                })
            return web.json_response({
                "success": False,
                "error": result.error or "Queue kosong",
            })
        except Exception as e:
            logger.error(f"queue_consume error: {e}")
            return web.json_response({"error": str(e)}, status=400)

    async def handle_queue_ack(self, request: web.Request) -> web.Response:
        try:
            data = await request.json()
            ok = await self.queue_node.acknowledge(
                data["message_id"],
                data.get("consumer_id", "default"),
            )
            return web.json_response({"success": ok})
        except Exception as e:
            logger.error(f"queue_ack error: {e}")
            return web.json_response({"error": str(e)}, status=400)

    async def handle_queue_status(self, request: web.Request) -> web.Response:
        return web.json_response({"queues": self.queue_node.get_status()})

    # ── CACHE ──────────────────────────────────────────────
    async def handle_cache_set(self, request: web.Request) -> web.Response:
        if r := self._leader_only():
            return r
        try:
            data = await request.json()
            ok = await self.cache_node.set(data["key"], data["value"], data.get("ttl"))
            return web.json_response({"success": ok})
        except Exception as e:
            logger.error(f"cache_set error: {e}")
            return web.json_response({"error": str(e)}, status=400)

    async def handle_cache_get(self, request: web.Request) -> web.Response:
        try:
            key = request.match_info["key"]
            hit, value = await self.cache_node.get(key)
            return web.json_response({"hit": hit, "key": key, "value": value})
        except Exception as e:
            logger.error(f"cache_get error: {e}")
            return web.json_response({"error": str(e)}, status=400)

    async def handle_cache_invalidate(self, request: web.Request) -> web.Response:
        if r := self._leader_only():
            return r
        try:
            key = request.match_info["key"]
            ok = await self.cache_node.invalidate(key)
            return web.json_response({"success": ok, "key": key})
        except Exception as e:
            logger.error(f"cache_invalidate error: {e}")
            return web.json_response({"error": str(e)}, status=400)

    async def handle_cache_status(self, request: web.Request) -> web.Response:
        return web.json_response(self.cache_node.get_status())


# ── ENTRYPOINT ─────────────────────────────────────────────
async def main():
    node = Node()
    await node.start()
    try:
        await asyncio.Event().wait()
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        await node.stop()


if __name__ == "__main__":
    asyncio.run(main())