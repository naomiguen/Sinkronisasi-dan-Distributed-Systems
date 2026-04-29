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

# Setup logging
logging.basicConfig(
    level=getattr(logging, config.log_level.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout
)
logger = logging.getLogger(__name__)


class Node:

    def __init__(self):
        self.node_id = config.node.node_id
        self.host = config.node.host
        self.port = config.node.port

        # Hitung peer IDs (semua node kecuali diri sendiri)
        cluster_size = config.cluster.cluster_size
        self.peer_ids = [i for i in range(1, cluster_size + 1) if i != self.node_id]

        logger.info(
            f"Inisialisasi Node {self.node_id} "
            f"(port={self.port}, peers={self.peer_ids})"
        )

        # Inisialisasi komponen
        self.raft = RaftNode(
            node_id=self.node_id,
            peers=self.peer_ids
        )
        self.message_passing = MessagePassing(self.raft)
        self.failure_detector = FailureDetector(self.node_id, self.peer_ids)

        # Web app
        self.app = web.Application()
        self._setup_routes()

        self.runner: Optional[web.AppRunner] = None

    def _setup_routes(self):
        """Daftarkan semua HTTP routes."""
        routes = [
            # Health check
            web.get("/health", self.handle_health),
            web.get("/status", self.handle_status),
        ]

        # Tambahkan Raft RPC routes dari MessagePassing
        routes += self.message_passing.get_routes()

        # Lock Manager routes (akan diisi setelah lock_manager.py dibuat)
        routes += [
            web.post("/lock/acquire", self.handle_lock_acquire),
            web.post("/lock/release", self.handle_lock_release),
            web.get("/lock/status", self.handle_lock_status),
        ]

        # Queue routes (akan diisi setelah queue_node.py dibuat)
        routes += [
            web.post("/queue/produce", self.handle_queue_produce),
            web.post("/queue/consume", self.handle_queue_consume),
            web.get("/queue/status", self.handle_queue_status),
        ]

        # Cache routes (akan diisi setelah cache_node.py dibuat)
        routes += [
            web.put("/cache/set", self.handle_cache_set),
            web.get("/cache/get/{key}", self.handle_cache_get),
            web.delete("/cache/invalidate/{key}", self.handle_cache_invalidate),
            web.get("/cache/status", self.handle_cache_status),
        ]

        self.app.add_routes(routes)

    
    # LIFECYCLE
    

    async def start(self):
        """Jalankan semua komponen node."""
        logger.info(f"Node {self.node_id} starting up...")

        # Start metrics server
        if config.metrics_enabled:
            metrics.start_server()

        # Start HTTP client session untuk RPC
        await self.message_passing.start()

        # Start failure detector
        await self.failure_detector.start()

        # Daftarkan callback saat ada node yang mati
        self.failure_detector.on_node_dead(self._on_peer_dead)
        self.failure_detector.on_node_recovered(self._on_peer_recovered)

        # Start Raft consensus
        await self.raft.start()

        # Start HTTP server
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, self.host, self.port)
        await site.start()

        logger.info(
            f"Node {self.node_id} ONLINE di http://{self.host}:{self.port}"
        )

    async def stop(self):
        """Hentikan semua komponen dengan bersih."""
        logger.info(f"Node {self.node_id} shutting down...")
        await self.raft.stop()
        await self.failure_detector.stop()
        await self.message_passing.stop()
        if self.runner:
            await self.runner.cleanup()

    async def _on_peer_dead(self, peer_id: int):
        """Callback: dipanggil saat ada peer yang dinyatakan mati."""
        logger.warning(f"Peer {peer_id} dinyatakan DEAD oleh failure detector")
        # Lock Manager akan melepas lock yang dipegang peer yang mati
        # (implementasi di lock_manager.py)

    async def _on_peer_recovered(self, peer_id: int):
        """Callback: dipanggil saat peer yang mati kembali online."""
        logger.info(f"Peer {peer_id} kembali ONLINE")

    
    # HELPER: redirect ke leader jika bukan leader
    def _redirect_to_leader(self) -> Optional[web.Response]:
        """
        Jika node ini bukan leader, return response 307 Redirect ke leader.
        Return None jika node ini adalah leader (lanjutkan proses normal).
        """
        if self.raft.is_leader():
            return None

        leader_id = self.raft.get_leader_id()
        if leader_id is None:
            return web.json_response(
                {"error": "Tidak ada leader saat ini, coba lagi nanti"},
                status=503
            )

        nodes = config.cluster.nodes
        if leader_id <= len(nodes):
            leader = nodes[leader_id - 1]
            leader_url = f"http://{leader['host']}:{leader['port']}"
            # Untuk simplifikasi, kita return info leader bukan redirect
            return web.json_response(
                {
                    "error": "Bukan leader",
                    "leader_id": leader_id,
                    "leader_url": leader_url,
                },
                status=307
            )

        return web.json_response(
            {"error": "Leader tidak diketahui"},
            status=503
        )

    
    # HEALTH & STATUS HANDLERS
    async def handle_health(self, request: web.Request) -> web.Response:
        """GET /health - dipakai Docker health check dan FailureDetector."""
        return web.json_response({
            "status": "ok",
            "node_id": self.node_id,
            "role": self.raft.role.value,
        })

    async def handle_status(self, request: web.Request) -> web.Response:
        """GET /status - info lengkap node untuk debugging."""
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

    
    # LOCK HANDLERS (stub - akan diimplementasi di lock_manager.py)
    async def handle_lock_acquire(self, request: web.Request) -> web.Response:
        redirect = self._redirect_to_leader()
        if redirect:
            return redirect
        # TODO: akan dihubungkan ke LockManager di langkah berikutnya
        return web.json_response({"error": "Lock manager belum diimplementasi"}, status=501)

    async def handle_lock_release(self, request: web.Request) -> web.Response:
        redirect = self._redirect_to_leader()
        if redirect:
            return redirect
        return web.json_response({"error": "Lock manager belum diimplementasi"}, status=501)

    async def handle_lock_status(self, request: web.Request) -> web.Response:
        return web.json_response({"locks": []})

    
    # QUEUE HANDLERS (stub)
    async def handle_queue_produce(self, request: web.Request) -> web.Response:
        redirect = self._redirect_to_leader()
        if redirect:
            return redirect
        return web.json_response({"error": "Queue belum diimplementasi"}, status=501)

    async def handle_queue_consume(self, request: web.Request) -> web.Response:
        return web.json_response({"error": "Queue belum diimplementasi"}, status=501)

    async def handle_queue_status(self, request: web.Request) -> web.Response:
        return web.json_response({"queues": []})

    
    # CACHE HANDLERS (stub)

    async def handle_cache_set(self, request: web.Request) -> web.Response:
        redirect = self._redirect_to_leader()
        if redirect:
            return redirect
        return web.json_response({"error": "Cache belum diimplementasi"}, status=501)

    async def handle_cache_get(self, request: web.Request) -> web.Response:
        key = request.match_info["key"]
        return web.json_response({"error": "Cache belum diimplementasi"}, status=501)

    async def handle_cache_invalidate(self, request: web.Request) -> web.Response:
        key = request.match_info["key"]
        return web.json_response({"error": "Cache belum diimplementasi"}, status=501)

    async def handle_cache_status(self, request: web.Request) -> web.Response:
        return web.json_response({"cache": {}})



# ENTRYPOINT
async def main():
    node = Node()
    await node.start()

    logger.info("Node berjalan. Tekan Ctrl+C untuk berhenti.")

    try:
        # Jaga agar program tetap berjalan
        await asyncio.Event().wait()
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        await node.stop()


if __name__ == "__main__":
    asyncio.run(main())