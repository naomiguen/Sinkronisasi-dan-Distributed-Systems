"""
PBFT (Practical Byzantine Fault Tolerance) Consensus Implementation

This module implements a PBFT consensus protocol that can tolerate Byzantine
faulty nodes (malicious or arbitrary behavior). The implementation follows
the classic PBFT algorithm with pre-prepare, prepare, and commit phases.

Requirements: n >= 3f + 1 where f is the number of Byzantine faults tolerated
"""

import asyncio
import hashlib
import json
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set
from collections import defaultdict
import random

logger = logging.getLogger(__name__)


class MessageType(Enum):
    """PBFT message types"""
    REQUEST = "request"
    PRE_PREPARE = "pre_prepare"
    PREPARE = "prepare"
    COMMIT = "commit"
    REPLY = "reply"
    VIEW_CHANGE = "view_change"
    NEW_VIEW = "new_view"
    CHECKPOINT = "checkpoint"


class RequestState(Enum):
    """State of a client request"""
    IDLE = "idle"
    PRE_PREPARED = "pre_prepared"
    PREPARED = "prepared"
    COMMITTED = "committed"
    EXECUTED = "executed"
    REPLIED = "replied"


@dataclass
class PBFTMessage:
    """Represents a PBFT protocol message"""
    msg_type: MessageType
    view: int
    sequence: int
    sender_id: str
    request: Optional[Dict[str, Any]] = None
    digest: Optional[str] = None
    signature: Optional[str] = None
    timestamp: float = field(default_factory=time.time)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": self.msg_type.value,
            "view": self.view,
            "sequence": self.sequence,
            "sender": self.sender_id,
            "request": self.request,
            "digest": self.digest,
            "signature": self.signature,
            "timestamp": self.timestamp
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'PBFTMessage':
        return cls(
            msg_type=MessageType(data["type"]),
            view=data["view"],
            sequence=data["sequence"],
            sender_id=data["sender"],
            request=data.get("request"),
            digest=data.get("digest"),
            signature=data.get("signature"),
            timestamp=data.get("timestamp", time.time())
        )


@dataclass
class RequestRecord:
    """Records the state of a client request in PBFT"""
    request: Dict[str, Any]
    digest: str
    state: RequestState = RequestState.IDLE
    pre_prepare_senders: Set[str] = field(default_factory=set)
    prepare_senders: Set[str] = field(default_factory=set)
    commit_senders: Set[str] = field(default_factory=set)
    replies: Dict[str, Any] = field(default_factory=dict)
    execution_result: Optional[Any] = None
    timestamp: float = field(default_factory=time.time)


class PBFTNode:
    """
    Implements a PBFT consensus node.
    
    PBFT can tolerate f Byzantine faults in a system of n nodes where n >= 3f + 1
    
    Protocol phases:
    1. Request: Client sends request to primary
    2. Pre-Prepare: Primary broadcasts to all replicas
    3. Prepare: All replicas exchange prepare messages
    4. Commit: All replicas exchange commit messages
    5. Reply: Replicas send reply to client
    """
    
    def __init__(
        self,
        node_id: str,
        all_nodes: List[str],
        f: int,
        execute_fn: Callable,
        send_to_peer: Callable,
        get_peer_id: Callable
    ):
        """
        Initialize PBFT node.
        
        Args:
            node_id: Unique identifier for this node
            all_nodes: List of all PBFT node IDs
            f: Maximum number of Byzantine faults to tolerate
            execute_fn: Function to execute client requests
            send_to_peer: Async function to send message to peer
            get_peer_id: Function to get current peer ID
        """
        self.node_id = node_id
        self.all_nodes = all_nodes
        self.n = len(all_nodes)
        self.f = f
        
        # Verify PBFT requirement: n >= 3f + 1
        if self.n < 3 * f + 1:
            raise ValueError(
                f"PBFT requires n >= 3f + 1. Got n={self.n}, f={f}"
            )
        
        self.execute_fn = execute_fn
        self.send_to_peer = send_to_peer
        self.get_peer_id = get_peer_id
        
        # View management (primary selection)
        self.view = 0
        self.is_primary = self._is_primary()
        
        # Sequence number management
        self.low_water_mark = 0
        self.high_water_mark = 100  # Could be dynamic
        self.last_executed_seq = 0
        
        # Request tracking
        self.pending_requests: Dict[str, RequestRecord] = {}
        self.request_history: Dict[int, RequestRecord] = {}
        
        # Message verification
        self.valid_messages: Dict[str, Set[str]] = defaultdict(set)
        
        # Checkpointing
        self.checkpoints: Dict[int, Dict[str, Any]] = {}
        self.stable_checkpoint_seq = 0
        
        # View change tracking
        self.view_change_in_progress = False
        self.view_change_messages: Dict[int, Dict[str, Any]] = {}
        
        # Timing
        self.request_timeout = 30.0  # seconds
        self.view_change_timeout = 5.0
        
        logger.info(
            f"PBFT Node {node_id} initialized: n={self.n}, f={self.f}, "
            f"primary={self.is_primary}"
        )
    
    def _is_primary(self) -> bool:
        """Determine if this node is primary for current view"""
        return self.all_nodes[self.view % self.n] == self.node_id
    
    def _compute_digest(self, data: Dict[str, Any]) -> str:
        """Compute SHA-256 digest of request data"""
        serialized = json.dumps(data, sort_keys=True)
        return hashlib.sha256(serialized.encode()).hexdigest()
    
    def _verify_digest(self, data: Dict[str, Any], digest: str) -> bool:
        """Verify digest matches data"""
        return self._compute_digest(data) == digest
    
    def _sign_message(self, message: PBFTMessage) -> str:
        """
        Sign message (simplified - in production use proper crypto)
        In production, use Ed25519 or RSA signatures
        """
        # Simplified signing - in production use proper cryptographic signing
        message_data = f"{message.msg_type.value}:{message.view}:{message.sequence}:{message.digest}"
        return hashlib.sha256(message_data.encode()).hexdigest()[:16]
    
    async def handle_request(self, request: Dict[str, Any]) -> Any:
        """
        Handle incoming client request.
        If this node is primary, start PBFT protocol.
        Otherwise, forward to primary.
        """
        request_id = request.get("request_id", f"{time.time()}_{random.randint(1000, 9999)}")
        
        if self.is_primary:
            return await self._start_consensus(request)
        else:
            # Forward to primary
            primary_id = self.all_nodes[self.view % self.n]
            await self.send_to_peer(primary_id, {
                "type": "forward_request",
                "request": request,
                "request_id": request_id
            })
            return {"status": "forwarded", "request_id": request_id}
    
    async def _start_consensus(self, request: Dict[str, Any]) -> Any:
        """Start PBFT consensus protocol as primary"""
        request_id = request.get("request_id", f"{time.time()}_{random.randint(1000, 9999)}")
        
        # Generate sequence number
        self.last_executed_seq += 1
        seq_num = self.last_executed_seq
        
        # Compute digest
        digest = self._compute_digest(request)
        
        # Create request record
        record = RequestRecord(
            request=request,
            digest=digest,
            state=RequestState.PRE_PREPARED
        )
        self.pending_requests[request_id] = record
        self.request_history[seq_num] = record
        
        # Send PRE-PREPARE to all replicas
        pre_prepare = PBFTMessage(
            msg_type=MessageType.PRE_PREPARE,
            view=self.view,
            sequence=seq_num,
            sender_id=self.node_id,
            request=request,
            digest=digest
        )
        pre_prepare.signature = self._sign_message(pre_prepare)
        
        for node_id in self.all_nodes:
            if node_id != self.node_id:
                await self.send_to_peer(node_id, pre_prepare.to_dict())
        
        # Also process locally
        await self._handle_pre_prepare(pre_prepare)
        
        return {"status": "pre_prepared", "request_id": request_id, "sequence": seq_num}
    
    async def _handle_pre_prepare(self, message: PBFTMessage) -> None:
        """Handle PRE-PREPARE message"""
        request_id = message.request.get("request_id", f"{message.sequence}")
        
        # Verify message
        if message.view != self.view:
            logger.warning(f"View mismatch in PRE-PREPARE: {message.view} != {self.view}")
            return
        
        if message.sequence <= self.low_water_mark or message.sequence > self.high_water_mark:
            logger.warning(f"Sequence out of range: {message.sequence}")
            return
        
        # Verify digest
        expected_digest = self._compute_digest(message.request)
        if message.digest != expected_digest:
            logger.error(f"Digest mismatch in PRE-PREPARE")
            return
        
        # Create or update request record
        if request_id not in self.pending_requests:
            record = RequestRecord(
                request=message.request,
                digest=message.digest,
                state=RequestState.PRE_PREPARED
            )
            self.pending_requests[request_id] = record
            self.request_history[message.sequence] = record
        else:
            record = self.pending_requests[request_id]
            record.state = RequestState.PRE_PREPARED
        
        record.pre_prepare_senders.add(message.sender_id)
        
        # Send PREPARE to all other replicas
        prepare = PBFTMessage(
            msg_type=MessageType.PREPARE,
            view=self.view,
            sequence=message.sequence,
            sender_id=self.node_id,
            digest=message.digest
        )
        prepare.signature = self._sign_message(prepare)
        
        for node_id in self.all_nodes:
            if node_id != self.node_id:
                await self.send_to_peer(node_id, prepare.to_dict())
        
        # Also process locally
        await self._handle_prepare(prepare)
    
    async def _handle_prepare(self, message: PBFTMessage) -> None:
        """Handle PREPARE message"""
        request_id = message.request.get("request_id") if message.request else f"{message.sequence}"
        
        # Verify message
        if message.view != self.view:
            return
        
        # Find the request record
        record = self.request_history.get(message.sequence)
        if not record:
            logger.warning(f"No record for sequence {message.sequence}")
            return
        
        # Verify digest matches
        if message.digest != record.digest:
            logger.error(f"Digest mismatch in PREPARE")
            return
        
        # Record prepare message
        record.prepare_senders.add(message.sender_id)
        
        # Check if we have enough prepare messages (2f + 1 including self)
        # In PBFT: need 2f + 1 prepares to enter prepared state
        if (len(record.prepare_senders) >= 2 * self.f + 1 and 
            record.state == RequestState.PRE_PREPARED):
            record.state = RequestState.PREPARED
            
            # Send COMMIT to all replicas
            commit = PBFTMessage(
                msg_type=MessageType.COMMIT,
                view=self.view,
                sequence=message.sequence,
                sender_id=self.node_id,
                digest=message.digest
            )
            commit.signature = self._sign_message(commit)
            
            for node_id in self.all_nodes:
                if node_id != self.node_id:
                    await self.send_to_peer(node_id, commit.to_dict())
            
            # Also process locally
            await self._handle_commit(commit)
    
    async def _handle_commit(self, message: PBFTMessage) -> None:
        """Handle COMMIT message"""
        request_id = message.request.get("request_id") if message.request else f"{message.sequence}"
        
        # Verify message
        if message.view != self.view:
            return
        
        # Find the request record
        record = self.request_history.get(message.sequence)
        if not record:
            return
        
        # Record commit message
        record.commit_senders.add(message.sender_id)
        
        # Check if we have enough commit messages (2f + 1 including self)
        if (len(record.commit_senders) >= 2 * self.f + 1 and 
            record.state == RequestState.PREPARED):
            record.state = RequestState.COMMITTED
            
            # Execute the request
            await self._execute_request(record, message.sequence)
    
    async def _execute_request(self, record: RequestRecord, sequence: int) -> None:
        """Execute the committed request"""
        try:
            # Execute the request
            result = await self.execute_fn(record.request)
            record.execution_result = result
            record.state = RequestState.EXECUTED
            
            # Update low water mark
            self.last_executed_seq = sequence
            if sequence - self.stable_checkpoint_seq > 50:
                await self._generate_checkpoint(sequence)
            
            # Send REPLY to client
            await self._send_reply(record.request.get("request_id"), result)
            
        except Exception as e:
            logger.error(f"Error executing request: {e}")
            record.execution_result = {"error": str(e)}
    
    async def _send_reply(self, request_id: str, result: Any) -> None:
        """Send reply to client"""
        reply = {
            "type": "reply",
            "request_id": request_id,
            "result": result,
            "view": self.view,
            "timestamp": time.time(),
            "node_id": self.node_id
        }
        
        # In a real system, send to client. Here we store locally.
        record = self.pending_requests.get(request_id)
        if record:
            record.replies[self.node_id] = reply
            record.state = RequestState.REPLIED
    
    async def handle_view_change(self, new_view: int) -> None:
        """
        Handle view change (primary replacement).
        Triggered when primary fails or timeout.
        """
        logger.info(f"Node {self.node_id} initiating view change to view {new_view}")
        
        self.view_change_in_progress = True
        
        # Collect checkpoint and prepare messages
        view_change_msg = {
            "type": "view_change",
            "view": new_view,
            "node_id": self.node_id,
            "stable_checkpoint": self.stable_checkpoint_seq,
            "last_executed": self.last_executed_seq,
            "prepare_messages": []
        }
        
        # Include prepare messages for requests in prepared or committed state
        for seq, record in self.request_history.items():
            if record.state in [RequestState.PREPARED, RequestState.COMMITTED]:
                view_change_msg["prepare_messages"].append({
                    "sequence": seq,
                    "digest": record.digest,
                    "request": record.request
                })
        
        # Send view change to all nodes
        for node_id in self.all_nodes:
            if node_id != self.node_id:
                await self.send_to_peer(node_id, view_change_msg)
    
    async def handle_new_view(self, new_view: int, checkpoint_info: Dict, prepare_info: List[Dict]) -> None:
        """Handle NEW_VIEW message from new primary"""
        logger.info(f"Node {self.node_id} received new view {new_view}")
        
        self.view = new_view
        self.is_primary = self._is_primary()
        self.view_change_in_progress = False
        
        # Update stable checkpoint
        self.stable_checkpoint_seq = checkpoint_info.get("sequence", self.stable_checkpoint_seq)
        
        # Re-execute any missing requests from prepare_info
        for prep in prepare_info:
            seq = prep["sequence"]
            if seq not in self.request_history:
                record = RequestRecord(
                    request=prep["request"],
                    digest=prep["digest"],
                    state=RequestState.COMMITTED
                )
                self.request_history[seq] = record
                await self._execute_request(record, seq)
    
    async def _generate_checkpoint(self, sequence: int) -> None:
        """Generate a checkpoint (stable state)"""
        # In production, this would checkpoint the actual state
        self.checkpoints[sequence] = {
            "sequence": sequence,
            "timestamp": time.time(),
            "node_id": self.node_id
        }
        
        # Send checkpoint to other nodes
        checkpoint_msg = {
            "type": "checkpoint",
            "sequence": sequence,
            "node_id": self.node_id,
            "timestamp": time.time()
        }
        
        for node_id in self.all_nodes:
            if node_id != self.node_id:
                await self.send_to_peer(node_id, checkpoint_msg)
    
    async def handle_message(self, message: Dict[str, Any]) -> Any:
        """Route incoming PBFT messages to appropriate handler"""
        msg_type = MessageType(message.get("type", ""))
        
        pbft_msg = PBFTMessage.from_dict(message)
        
        switch = {
            MessageType.REQUEST: lambda: self.handle_request(message.get("request", {})),
            MessageType.PRE_PREPARE: lambda: self._handle_pre_prepare(pbft_msg),
            MessageType.PREPARE: lambda: self._handle_prepare(pbft_msg),
            MessageType.COMMIT: lambda: self._handle_commit(pbft_msg),
            MessageType.VIEW_CHANGE: lambda: self.handle_view_change(message.get("view", 0)),
            MessageType.NEW_VIEW: lambda: self.handle_new_view(
                message.get("view", 0),
                message.get("checkpoint_info", {}),
                message.get("prepare_info", [])
            ),
            MessageType.CHECKPOINT: lambda: self._handle_checkpoint(message),
        }
        
        handler = switch.get(msg_type)
        if handler:
            return await handler()
        
        logger.warning(f"Unknown message type: {msg_type}")
        return None
    
    async def _handle_checkpoint(self, message: Dict[str, Any]) -> None:
        """Handle checkpoint message from other nodes"""
        seq = message.get("sequence", 0)
        sender = message.get("node_id", "")
        
        # In production, collect 2f + 1 checkpoints to mark stable
        # For simplicity, just log here
        logger.debug(f"Received checkpoint {seq} from {sender}")
    
    def get_status(self) -> Dict[str, Any]:
        """Get current PBFT node status"""
        return {
            "node_id": self.node_id,
            "view": self.view,
            "is_primary": self.is_primary,
            "n": self.n,
            "f": self.f,
            "last_executed_seq": self.last_executed_seq,
            "pending_requests": len(self.pending_requests),
            "stable_checkpoint": self.stable_checkpoint_seq
        }


class ByzantineDetector:
    """
    Detects Byzantine (malicious) behavior in PBFT nodes.
    Monitors for:
    - Inconsistent messages
    - Invalid signatures
    - View changes without cause
    - Duplicate messages
    """
    
    def __init__(self, node_id: str, f: int):
        self.node_id = node_id
        self.f = f
        self.suspicious_nodes: Dict[str, int] = defaultdict(int)
        self.message_history: Dict[str, List[Dict]] = defaultdict(list)
        self.byzantine_threshold = f + 1
    
    def check_message_validity(self, sender: str, message: Dict) -> bool:
        """
        Check if message is valid and not from a Byzantine node.
        Returns False if message appears malicious.
        """
        msg_key = f"{sender}:{message.get('type')}:{message.get('sequence')}"
        
        # Check for duplicate messages
        if msg_key in self.message_history:
            prev = self.message_history[msg_key]
            if prev != message:
                # Inconsistent message from same sender - Byzantine behavior
                self.suspicious_nodes[sender] += 1
                logger.warning(f"Byzantine behavior detected from {sender}: inconsistent messages")
                return False
        
        self.message_history[msg_key] = message
        
        # Check signature (simplified)
        if not message.get("signature"):
            self.suspicious_nodes[sender] += 1
            logger.warning(f"Byzantine behavior detected from {sender}: missing signature")
            return False
        
        return True
    
    def is_suspicious(self, node_id: str) -> bool:
        """Check if a node is suspected to be Byzantine"""
        return self.suspicious_nodes[node_id] >= self.byzantine_threshold
    
    def get_suspicious_nodes(self) -> List[str]:
        """Get list of suspected Byzantine nodes"""
        return [n for n, count in self.suspicious_nodes.items() 
                if count >= self.byzantine_threshold]


# Example usage and testing
async def test_pbft():
    """Test PBFT implementation with mock nodes"""
    
    nodes = ["node1", "node2", "node3", "node4"]  # n=4, can tolerate f=1
    
    results = {}
    
    async def mock_execute(request):
        await asyncio.sleep(0.01)  # Simulate execution
        return {"result": f"executed: {request.get('command')}"}
    
    async def mock_send(peer_id, message):
        # In real system, would actually send
        pass
    
    def mock_get_peer_id():
        return "node1"
    
    # Create PBFT nodes
    pbft_nodes = {}
    for node_id in nodes:
        pbft_nodes[node_id] = PBFTNode(
            node_id=node_id,
            all_nodes=nodes,
            f=1,
            execute_fn=mock_execute,
            send_to_peer=mock_send,
            get_peer_id=mock_get_peer_id
        )
    
    # Test request
    primary = pbft_nodes[nodes[0]]
    result = await primary.handle_request({
        "command": "test_operation",
        "request_id": "test_001"
    })
    
    print(f"PBFT Test Result: {result}")
    print(f"Primary Status: {primary.get_status()}")
    
    return result


if __name__ == "__main__":
    asyncio.run(test_pbft())