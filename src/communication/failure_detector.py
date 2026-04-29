import asyncio
import time
import logging
from typing import Dict, Set, Optional
from enum import Enum

import aiohttp

from src.utils.config import config

logger = logging.getLogger(__name__)


class NodeStatus(Enum):
    ALIVE   = "alive"
    SUSPECT = "suspect"   # belum merespons, tapi belum pasti mati
    DEAD    = "dead"      # sudah beberapa kali tidak merespons


class FailureDetector:
    PING_INTERVAL    = 1.0   # detik - seberapa sering ping
    SUSPECT_AFTER    = 3     # berapa kali gagal sebelum SUSPECT
    DEAD_AFTER       = 6     # berapa kali gagal sebelum DEAD

    def __init__(self, node_id: int, peer_ids: list):
        self.node_id = node_id
        self.peer_ids = peer_ids

        # Status tiap peer
        self._status: Dict[int, NodeStatus] = {
            pid: NodeStatus.ALIVE for pid in peer_ids
        }
        # Jumlah gagal berturut-turut
        self._fail_count: Dict[int, int] = {pid: 0 for pid in peer_ids}
        # Waktu terakhir berhasil di-ping
        self._last_seen: Dict[int, float] = {
            pid: time.time() for pid in peer_ids
        }

        self._session: Optional[aiohttp.ClientSession] = None
        self._running = False
        self._task: Optional[asyncio.Task] = None

        # Callback saat ada perubahan status node
        self._on_node_dead: Optional[callable] = None
        self._on_node_recovered: Optional[callable] = None

    async def start(self):
        """Mulai background loop untuk health check."""
        self._running = True
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=2.0)
        )
        self._task = asyncio.create_task(self._ping_loop())
        logger.info(
            f"FailureDetector node {self.node_id} started, "
            f"monitoring peers={self.peer_ids}"
        )

    async def stop(self):
        """Hentikan health check."""
        self._running = False
        if self._task:
            self._task.cancel()
        if self._session:
            await self._session.close()

    def on_node_dead(self, callback):
        """Daftarkan callback yang dipanggil saat node dinyatakan DEAD."""
        self._on_node_dead = callback

    def on_node_recovered(self, callback):
        """Daftarkan callback yang dipanggil saat node kembali ALIVE."""
        self._on_node_recovered = callback

    def is_alive(self, node_id: int) -> bool:
        return self._status.get(node_id) == NodeStatus.ALIVE

    def is_dead(self, node_id: int) -> bool:
        return self._status.get(node_id) == NodeStatus.DEAD

    def get_alive_peers(self) -> Set[int]:
        return {pid for pid in self.peer_ids if self.is_alive(pid)}

    def get_status_all(self) -> Dict[int, str]:
        return {pid: s.value for pid, s in self._status.items()}

    async def _ping_loop(self):
        """Loop utama: ping semua peer setiap PING_INTERVAL detik."""
        while self._running:
            tasks = [self._ping_peer(pid) for pid in self.peer_ids]
            await asyncio.gather(*tasks, return_exceptions=True)
            await asyncio.sleep(self.PING_INTERVAL)

    async def _ping_peer(self, peer_id: int):
        """Ping satu peer via HTTP GET /health."""
        nodes = config.cluster.nodes
        if peer_id > len(nodes):
            return

        node_cfg = nodes[peer_id - 1]
        url = f"http://{node_cfg['host']}:{node_cfg['port']}/health"

        prev_status = self._status[peer_id]

        try:
            async with self._session.get(url) as resp:
                if resp.status == 200:
                    # Berhasil
                    self._fail_count[peer_id] = 0
                    self._last_seen[peer_id] = time.time()

                    if prev_status != NodeStatus.ALIVE:
                        logger.info(f"Node {peer_id} RECOVERED (kembali online)")
                        self._status[peer_id] = NodeStatus.ALIVE
                        if self._on_node_recovered:
                            await self._on_node_recovered(peer_id)
                else:
                    self._handle_failure(peer_id, prev_status)

        except Exception:
            self._handle_failure(peer_id, prev_status)

    def _handle_failure(self, peer_id: int, prev_status: NodeStatus):
        """Update status setelah ping gagal."""
        self._fail_count[peer_id] += 1
        fails = self._fail_count[peer_id]

        if fails >= self.DEAD_AFTER:
            new_status = NodeStatus.DEAD
        elif fails >= self.SUSPECT_AFTER:
            new_status = NodeStatus.SUSPECT
        else:
            new_status = NodeStatus.ALIVE

        self._status[peer_id] = new_status

        if prev_status != NodeStatus.DEAD and new_status == NodeStatus.DEAD:
            logger.warning(
                f"Node {peer_id} dinyatakan DEAD "
                f"(tidak merespons {fails} kali berturut-turut)"
            )
            if self._on_node_dead:
                asyncio.create_task(self._on_node_dead(peer_id))
        elif new_status == NodeStatus.SUSPECT and prev_status == NodeStatus.ALIVE:
            logger.info(f"Node {peer_id} SUSPECT (gagal {fails}x)")