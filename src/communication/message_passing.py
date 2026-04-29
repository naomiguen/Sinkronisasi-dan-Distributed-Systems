import asyncio
import logging
from typing import Optional, Dict, Any, List

import aiohttp
from aiohttp import web

from src.utils.config import config

logger = logging.getLogger(__name__)


class MessagePassing:
  
    def __init__(self, raft_node):
        self.raft_node = raft_node
        self.node_id = raft_node.node_id
        self.session: Optional[aiohttp.ClientSession] = None

        # Map node_id → URL, dibangun dari CLUSTER_NODES di config
        self.peer_urls: Dict[int, str] = self._build_peer_urls()

        # Daftarkan diri sebagai rpc_sender di raft node
        self.raft_node.rpc_sender = self

        self._timeout = aiohttp.ClientTimeout(
            total=config.raft.rpc_timeout / 1000.0
        )

    def _build_peer_urls(self) -> Dict[int, str]:
        urls = {}
        nodes = config.cluster.nodes
        for i, node in enumerate(nodes):
            node_id = i + 1  # node_id mulai dari 1
            if node_id != self.node_id:  # skip diri sendiri
                urls[node_id] = f"http://{node['host']}:{node['port']}"
        return urls

    async def start(self):
        """Buka HTTP session untuk outgoing requests."""
        connector = aiohttp.TCPConnector(limit=50)
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=self._timeout
        )
        logger.info(
            f"MessagePassing node {self.node_id} started, "
            f"peers={list(self.peer_urls.keys())}"
        )

    async def stop(self):
        """Tutup HTTP session."""
        if self.session:
            await self.session.close()

    
    # OUTGOING RPC (dikirim ke peer)
    async def send_vote_request(
        self, peer_id: int, request
    ) -> Optional[Dict]:
        """
        Kirim RequestVote ke peer.
        Dipanggil oleh RaftNode._request_vote_from()
        """
        url = self.peer_urls.get(peer_id)
        if not url:
            logger.warning(f"URL untuk peer {peer_id} tidak ditemukan")
            return None

        payload = {
            "term": request.term,
            "candidate_id": request.candidate_id,
            "last_log_index": request.last_log_index,
            "last_log_term": request.last_log_term,
        }

        return await self._post(peer_id, f"{url}/raft/vote", payload)

    async def send_append_entries(
        self, peer_id: int, request
    ) -> Optional[Dict]:
        """
        Kirim AppendEntries ke peer.
        Dipanggil oleh RaftNode._send_append_entries()
        """
        url = self.peer_urls.get(peer_id)
        if not url:
            return None

        payload = {
            "term": request.term,
            "leader_id": request.leader_id,
            "prev_log_index": request.prev_log_index,
            "prev_log_term": request.prev_log_term,
            "entries": request.entries,
            "leader_commit": request.leader_commit,
        }

        return await self._post(peer_id, f"{url}/raft/append", payload)

    async def _post(
        self, peer_id: int, url: str, payload: Dict
    ) -> Optional[Dict]:
        """Helper: POST JSON ke URL dan return response dict."""
        if not self.session:
            return None
        try:
            async with self.session.post(url, json=payload) as resp:
                if resp.status == 200:
                    return await resp.json()
                logger.debug(f"Peer {peer_id} respons {resp.status} untuk {url}")
                return None
        except asyncio.TimeoutError:
            logger.debug(f"Timeout RPC ke peer {peer_id} ({url})")
            return None
        except aiohttp.ClientConnectorError:
            logger.debug(f"Peer {peer_id} tidak bisa dihubungi ({url})")
            return None
        except Exception as e:
            logger.debug(f"Error RPC ke peer {peer_id}: {e}")
            return None

    # INCOMING RPC HANDLERS (untuk aiohttp router)
    async def handle_vote_request(self, request: web.Request) -> web.Response:
        """
        Handler HTTP POST /raft/vote
        Dipanggil saat ada candidate yang minta suara kita.
        """
        try:
            data = await request.json()
            from src.consensus.raft import VoteRequest
            vote_req = VoteRequest(
                term=data["term"],
                candidate_id=data["candidate_id"],
                last_log_index=data["last_log_index"],
                last_log_term=data["last_log_term"],
            )
            response = await self.raft_node.handle_vote_request(vote_req)
            return web.json_response({
                "term": response.term,
                "vote_granted": response.vote_granted,
            })
        except Exception as e:
            logger.error(f"Error handle_vote_request: {e}")
            return web.json_response({"error": str(e)}, status=500)

    async def handle_append_entries(self, request: web.Request) -> web.Response:
        """
        Handler HTTP POST /raft/append
        Dipanggil saat ada AppendEntries dari leader.
        """
        try:
            data = await request.json()
            from src.consensus.raft import AppendEntriesRequest
            append_req = AppendEntriesRequest(
                term=data["term"],
                leader_id=data["leader_id"],
                prev_log_index=data["prev_log_index"],
                prev_log_term=data["prev_log_term"],
                entries=data["entries"],
                leader_commit=data["leader_commit"],
            )
            response = await self.raft_node.handle_append_entries(append_req)
            return web.json_response({
                "term": response.term,
                "success": response.success,
                "match_index": response.match_index,
            })
        except Exception as e:
            logger.error(f"Error handle_append_entries: {e}")
            return web.json_response({"error": str(e)}, status=500)

    def get_routes(self) -> List:
        """Return list of aiohttp routes untuk didaftarkan ke web app."""
        return [
            web.post("/raft/vote", self.handle_vote_request),
            web.post("/raft/append", self.handle_append_entries),
        ]