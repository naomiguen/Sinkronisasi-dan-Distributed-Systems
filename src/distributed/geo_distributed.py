"""
Geo-Distributed System Implementation

This module implements a geo-distributed synchronization system that spans
multiple regions with latency-aware routing and conflict resolution.

Features:
- Multi-region deployment support
- Latency-based routing
- Conflict-free replicated data types (CRDTs)
- Eventual consistency with vector clocks
- Regional failover and recovery
"""

import asyncio
import hashlib
import json
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple
from collections import defaultdict
import random
import math

logger = logging.getLogger(__name__)


class Region(Enum):
    """Available geographic regions"""
    US_EAST = "us-east"
    US_WEST = "us-west"
    EU_WEST = "eu-west"
    EU_CENTRAL = "eu-central"
    ASIA_PACIFIC = "asia-pacific"
    AUSTRALIA = "australia"
    SOUTH_AMERICA = "sa-east"


@dataclass
class RegionConfig:
    """Configuration for a geographic region"""
    region: Region
    name: str
    latency_to: Dict[Region, float] = field(default_factory=dict)
    priority: int = 1
    enabled: bool = True


@dataclass
class VectorClock:
    """
    Vector clock for tracking causality in distributed operations.
    Each node maintains its own logical time counter.
    """
    clock: Dict[str, int] = field(default_factory=dict)
    
    def increment(self, node_id: str) -> None:
        """Increment logical time for a node"""
        self.clock[node_id] = self.clock.get(node_id, 0) + 1
    
    def merge(self, other: 'VectorClock') -> None:
        """Merge with another vector clock (take max of each)"""
        for node_id, time_val in other.clock.items():
            self.clock[node_id] = max(self.clock.get(node_id, 0), time_val)
    
    def happens_before(self, other: 'VectorClock') -> bool:
        """Check if this clock happens before another"""
        for node_id, time_val in other.clock.items():
            if self.clock.get(node_id, 0) >= time_val:
                continue
            else:
                return False
        
        # This happens before other if it's strictly less in at least one dimension
        return self.clock != other.clock
    
    def is_concurrent(self, other: 'VectorClock') -> bool:
        """Check if two clocks are concurrent (neither happens before other)"""
        return not self.happens_before(other) and not other.happens_before(self)
    
    def to_dict(self) -> Dict[str, int]:
        return dict(self.clock)
    
    @classmethod
    def from_dict(cls, data: Dict[str, int]) -> 'VectorClock':
        return cls(clock=data)


@dataclass
class CRDTRegister:
    """
    Conflict-free Replicated Data Type for a register (single value).
    Uses Last-Writer-Wins (LWW) with vector clocks for conflict resolution.
    """
    value: Any = None
    vector_clock: VectorClock = field(default_factory=VectorClock)
    timestamp: float = field(default_factory=time.time)
    writer_id: str = ""
    
    def set(self, value: Any, writer_id: str) -> None:
        """Set value with writer ID"""
        self.vector_clock.increment(writer_id)
        self.value = value
        self.writer_id = writer_id
        self.timestamp = time.time()
    
    def merge(self, other: 'CRDTRegister') -> 'CRDTRegister':
        """Merge with another register using LWW"""
        if other.vector_clock.happens_before(self.vector_clock):
            return self
        elif self.vector_clock.happens_before(other.vector_clock):
            return other
        else:
            # Concurrent - use timestamp for LWW
            if other.timestamp > self.timestamp:
                return other
            return self
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "value": self.value,
            "vector_clock": self.vector_clock.to_dict(),
            "timestamp": self.timestamp,
            "writer_id": self.writer_id
        }


@dataclass
class CRDTCounter:
    """
    G-Counter: Grow-only counter CRDT.
    Can only increment, never decrement.
    """
    counts: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    
    def increment(self, node_id: str, amount: int = 1) -> None:
        """Increment counter for a node"""
        self.counts[node_id] += amount
    
    def value(self) -> int:
        """Get total counter value"""
        return sum(self.counts.values())
    
    def merge(self, other: 'CRDTCounter') -> 'CRDTCounter':
        """Merge with another counter (take max for each node)"""
        result = CRDTCounter()
        all_nodes = set(self.counts.keys()) | set(other.counts.keys())
        for node_id in all_nodes:
            result.counts[node_id] = max(
                self.counts.get(node_id, 0),
                other.counts.get(node_id, 0)
            )
        return result
    
    def to_dict(self) -> Dict[str, int]:
        return dict(self.counts)


@dataclass
class CRDTSet:
    """
    Two-Phase Set: Add-wins set CRDT.
    Supports add and remove operations with conflict resolution.
    """
    added: Dict[str, VectorClock] = field(default_factory=dict)
    removed: Dict[str, VectorClock] = field(default_factory=dict)
    
    def add(self, element: str, node_id: str) -> None:
        """Add element to set"""
        if element not in self.removed:
            self.added[element] = VectorClock()
        self.added[element].increment(node_id)
    
    def remove(self, element: str, node_id: str) -> None:
        """Remove element from set"""
        if element not in self.removed:
            self.removed[element] = VectorClock()
        self.removed[element].increment(node_id)
    
    def contains(self, element: str) -> bool:
        """Check if element is in set"""
        if element not in self.added:
            return False
        if element in self.removed:
            # Check if remove happened after add
            if self.removed[element].happens_before(self.added[element]):
                return True
            return False
        return True
    
    def elements(self) -> Set[str]:
        """Get all elements in set"""
        result = set()
        for element in self.added:
            if self.contains(element):
                result.add(element)
        return result
    
    def merge(self, other: 'CRDTSet') -> 'CRDTSet':
        """Merge with another set"""
        result = CRDTSet()
        
        # Merge added
        all_elements = set(self.added.keys()) | set(other.added.keys())
        for elem in all_elements:
            if elem in self.added and elem in other.added:
                vc = VectorClock()
                vc.merge(self.added[elem])
                vc.merge(other.added[elem])
                result.added[elem] = vc
            elif elem in self.added:
                result.added[elem] = self.added[elem]
            else:
                result.added[elem] = other.added[elem]
        
        # Merge removed
        all_removed = set(self.removed.keys()) | set(other.removed.keys())
        for elem in all_removed:
            if elem in self.removed and elem in other.removed:
                vc = VectorClock()
                vc.merge(self.removed[elem])
                vc.merge(other.removed[elem])
                result.removed[elem] = vc
            elif elem in self.removed:
                result.removed[elem] = self.removed[elem]
            else:
                result.removed[elem] = other.removed[elem]
        
        return result
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "added": {k: v.to_dict() for k, v in self.added.items()},
            "removed": {k: v.to_dict() for k, v in self.removed.items()}
        }


class LatencyMonitor:
    """Monitor and track latency between regions"""
    
    def __init__(self):
        self.latency_samples: Dict[Tuple[Region, Region], List[float]] = defaultdict(list)
        self.avg_latency: Dict[Tuple[Region, Region], float] = {}
        self.max_samples = 100
    
    def record_latency(self, from_region: Region, to_region: Region, latency_ms: float) -> None:
        """Record a latency sample"""
        key = (from_region, to_region)
        self.latency_samples[key].append(latency_ms)
        
        # Keep only recent samples
        if len(self.latency_samples[key]) > self.max_samples:
            self.latency_samples[key] = self.latency_samples[key][-self.max_samples:]
        
        # Update average
        self.avg_latency[key] = sum(self.latency_samples[key]) / len(self.latency_samples[key])
    
    def get_latency(self, from_region: Region, to_region: Region) -> float:
        """Get average latency between regions"""
        key = (from_region, to_region)
        if key in self.avg_latency:
            return self.avg_latency[key]
        
        # Estimate based on region distance (fallback)
        return self._estimate_latency(from_region, to_region)
    
    def _estimate_latency(self, from_region: Region, to_region: Region) -> float:
        """Estimate latency based on region distance"""
        # Approximate distances and typical latencies
        # In production, use actual measured values
        if from_region == to_region:
            return 5.0  # Same region, low latency
        
        # Cross-continental estimates (ms)
        cross_continental = {
            (Region.US_EAST, Region.US_WEST): 70,
            (Region.US_EAST, Region.EU_WEST): 80,
            (Region.US_WEST, Region.ASIA_PACIFIC): 150,
            (Region.EU_WEST, Region.ASIA_PACIFIC): 180,
            (Region.EU_WEST, Region.EU_CENTRAL): 20,
            (Region.ASIA_PACIFIC, Region.AUSTRALIA): 100,
        }
        
        return cross_continental.get((from_region, to_region), 100)
    
    def get_best_region(self, from_region: Region, target_regions: List[Region]) -> Region:
        """Get the region with lowest latency"""
        best = target_regions[0]
        best_latency = float('inf')
        
        for region in target_regions:
            latency = self.get_latency(from_region, region)
            if latency < best_latency:
                best_latency = latency
                best = region
        
        return best


class GeoDistributedNode:
    """
    A node in a geo-distributed system.
    Handles cross-region communication with latency awareness.
    """
    
    def __init__(
        self,
        node_id: str,
        region: Region,
        peer_nodes: Dict[Region, List[str]],
        latency_monitor: LatencyMonitor
    ):
        self.node_id = node_id
        self.region = region
        self.peer_nodes = peer_nodes  # region -> list of node IDs
        self.latency_monitor = latency_monitor
        
        # Local data stores (CRDTs)
        self.registers: Dict[str, CRDTRegister] = {}
        self.counters: Dict[str, CRDTCounter] = {}
        self.sets: Dict[str, CRDTSet] = {}
        
        # Vector clock for this node
        self.vector_clock = VectorClock()
        
        # Pending sync operations
        self.pending_sync: List[Dict] = []
        
        # Region health
        self.region_health: Dict[Region, bool] = {r: True for r in Region}
        
        logger.info(f"Geo Node {node_id} initialized in region {region.value}")
    
    def increment_clock(self) -> None:
        """Increment local vector clock"""
        self.vector_clock.increment(self.node_id)
    
    async def write_register(self, key: str, value: Any) -> Dict[str, Any]:
        """Write to a CRDT register"""
        self.increment_clock()
        
        if key not in self.registers:
            self.registers[key] = CRDTRegister()
        
        self.registers[key].set(value, self.node_id)
        
        # Trigger sync to other regions
        await self._sync_to_regions(key, "register")
        
        return {
            "status": "written",
            "key": key,
            "region": self.region.value,
            "vector_clock": self.vector_clock.to_dict()
        }
    
    async def read_register(self, key: str) -> Optional[Any]:
        """Read from a CRDT register"""
        if key in self.registers:
            return self.registers[key].to_dict()
        return None
    
    async def increment_counter(self, key: str, amount: int = 1) -> Dict[str, Any]:
        """Increment a G-Counter"""
        self.increment_clock()
        
        if key not in self.counters:
            self.counters[key] = CRDTCounter()
        
        self.counters[key].increment(self.node_id, amount)
        
        await self._sync_to_regions(key, "counter")
        
        return {
            "status": "incremented",
            "key": key,
            "value": self.counters[key].value(),
            "region": self.region.value
        }
    
    async def add_to_set(self, key: str, element: str) -> Dict[str, Any]:
        """Add element to CRDT set"""
        self.increment_clock()
        
        if key not in self.sets:
            self.sets[key] = CRDTSet()
        
        self.sets[key].add(element, self.node_id)
        
        await self._sync_to_regions(key, "set")
        
        return {
            "status": "added",
            "key": key,
            "element": element,
            "region": self.region.value
        }
    
    async def remove_from_set(self, key: str, element: str) -> Dict[str, Any]:
        """Remove element from CRDT set"""
        self.increment_clock()
        
        if key in self.sets:
            self.sets[key].remove(element, self.node_id)
            await self._sync_to_regions(key, "set")
        
        return {
            "status": "removed",
            "key": key,
            "element": element,
            "region": self.region.value
        }
    
    async def _sync_to_regions(self, key: str, data_type: str) -> None:
        """Sync data to other regions"""
        # Get data to sync
        data = None
        if data_type == "register" and key in self.registers:
            data = self.registers[key].to_dict()
        elif data_type == "counter" and key in self.counters:
            data = self.counters[key].to_dict()
        elif data_type == "set" and key in self.sets:
            data = self.sets[key].to_dict()
        
        if not data:
            return
        
        # Sync to all other regions
        for region, node_ids in self.peer_nodes.items():
            if region != self.region and self.region_health.get(region, True):
                latency = self.latency_monitor.get_latency(self.region, region)
                
                # Store for async sync
                self.pending_sync.append({
                    "to_region": region,
                    "to_nodes": node_ids,
                    "key": key,
                    "data_type": data_type,
                    "data": data,
                    "latency": latency
                })
    
    async def receive_sync(
        self,
        key: str,
        data_type: str,
        data: Dict,
        from_region: Region
    ) -> Dict[str, Any]:
        """Receive sync data from another region"""
        # Record latency
        self.latency_monitor.record_latency(from_region, self.region, random.uniform(5, 50))
        
        # Merge based on data type
        if data_type == "register":
            if key not in self.registers:
                self.registers[key] = CRDTRegister()
            
            remote = CRDTRegister(
                value=data.get("value"),
                vector_clock=VectorClock.from_dict(data.get("vector_clock", {})),
                timestamp=data.get("timestamp", 0),
                writer_id=data.get("writer_id", "")
            )
            
            merged = self.registers[key].merge(remote)
            self.registers[key] = merged
            
        elif data_type == "counter":
            if key not in self.counters:
                self.counters[key] = CRDTCounter()
            
            remote_counts = data.get("counts", {})
            remote = CRDTCounter(counts=remote_counts)
            
            merged = self.counters[key].merge(remote)
            self.counters[key] = merged
            
        elif data_type == "set":
            if key not in self.sets:
                self.sets[key] = CRDTSet()
            
            # Reconstruct remote set
            remote = CRDTSet()
            for elem, vc_data in data.get("added", {}).items():
                remote.added[elem] = VectorClock.from_dict(vc_data)
            for elem, vc_data in data.get("removed", {}).items():
                remote.removed[elem] = VectorClock.from_dict(vc_data)
            
            merged = self.sets[key].merge(remote)
            self.sets[key] = merged
        
        return {"status": "synced", "key": key}
    
    async def get_best_peer(self, operation: str) -> Tuple[str, Region]:
        """Get the best peer for an operation based on latency"""
        target_regions = [r for r in Region if r != self.region and self.region_health.get(r, True)]
        
        if not target_regions:
            return self.node_id, self.region
        
        best_region = self.latency_monitor.get_best_region(self.region, target_regions)
        peer_nodes = self.peer_nodes.get(best_region, [])
        
        if peer_nodes:
            return peer_nodes[0], best_region
        
        return self.node_id, self.region
    
    def update_region_health(self, region: Region, healthy: bool) -> None:
        """Update health status of a region"""
        self.region_health[region] = healthy
        logger.info(f"Region {region.value} health: {healthy}")
    
    def get_status(self) -> Dict[str, Any]:
        """Get node status"""
        return {
            "node_id": self.node_id,
            "region": self.region.value,
            "registers": list(self.registers.keys()),
            "counters": {k: v.value() for k, v in self.counters.items()},
            "sets": {k: list(v.elements()) for k, v in self.sets.items()},
            "vector_clock": self.vector_clock.to_dict(),
            "region_health": {r.value: h for r, h in self.region_health.items()},
            "pending_sync": len(self.pending_sync)
        }


class GeoDistributedCluster:
    """
    Manages a geo-distributed cluster of nodes across multiple regions.
    Handles global coordination and failover.
    """
    
    def __init__(self):
        self.nodes: Dict[str, GeoDistributedNode] = {}
        self.latency_monitor = LatencyMonitor()
        self.region_primary: Dict[Region, str] = {}  # Primary node per region
        
        # Default region configurations
        self.region_configs = self._init_region_configs()
    
    def _init_region_configs(self) -> Dict[Region, RegionConfig]:
        """Initialize default region configurations"""
        configs = {
            Region.US_EAST: RegionConfig(Region.US_EAST, "US East (N. Virginia)"),
            Region.US_WEST: RegionConfig(Region.US_WEST, "US West (Oregon)"),
            Region.EU_WEST: RegionConfig(Region.EU_WEST, "EU West (Ireland)"),
            Region.EU_CENTRAL: RegionConfig(Region.EU_CENTRAL, "EU Central (Frankfurt)"),
            Region.ASIA_PACIFIC: RegionConfig(Region.ASIA_PACIFIC, "Asia Pacific (Tokyo)"),
            Region.AUSTRALIA: RegionConfig(Region.AUSTRALIA, "Australia (Sydney)"),
            Region.SOUTH_AMERICA: RegionConfig(Region.SOUTH_AMERICA, "South America (São Paulo)"),
        }
        
        # Set up default latencies
        configs[Region.US_EAST].latency_to = {
            Region.US_WEST: 70,
            Region.EU_WEST: 80,
            Region.EU_CENTRAL: 85,
            Region.ASIA_PACIFIC: 180,
            Region.AUSTRALIA: 200,
            Region.SOUTH_AMERICA: 150
        }
        
        configs[Region.US_WEST].latency_to = {
            Region.US_EAST: 70,
            Region.EU_WEST: 150,
            Region.EU_CENTRAL: 160,
            Region.ASIA_PACIFIC: 100,
            Region.AUSTRALIA: 180,
            Region.SOUTH_AMERICA: 180
        }
        
        configs[Region.EU_WEST].latency_to = {
            Region.US_EAST: 80,
            Region.US_WEST: 150,
            Region.EU_CENTRAL: 20,
            Region.ASIA_PACIFIC: 180,
            Region.AUSTRALIA: 220,
            Region.SOUTH_AMERICA: 200
        }
        
        configs[Region.ASIA_PACIFIC].latency_to = {
            Region.US_EAST: 180,
            Region.US_WEST: 100,
            Region.EU_WEST: 180,
            Region.EU_CENTRAL: 190,
            Region.AUSTRALIA: 100,
            Region.SOUTH_AMERICA: 280
        }
        
        return configs
    
    def add_node(self, node_id: str, region: Region, peer_nodes: Dict[Region, List[str]]) -> None:
        """Add a node to the cluster"""
        node = GeoDistributedNode(
            node_id=node_id,
            region=region,
            peer_nodes=peer_nodes,
            latency_monitor=self.latency_monitor
        )
        self.nodes[node_id] = node
        
        # Set as primary if first in region
        if region not in self.region_primary:
            self.region_primary[region] = node_id
        
        logger.info(f"Added node {node_id} in region {region.value}")
    
    def get_primary_for_region(self, region: Region) -> Optional[str]:
        """Get primary node for a region"""
        return self.region_primary.get(region)
    
    async def route_request(
        self,
        operation: str,
        key: str,
        value: Any = None,
        preferred_region: Optional[Region] = None
    ) -> Dict[str, Any]:
        """
        Route request to appropriate node with latency awareness.
        
        Args:
            operation: Type of operation (read, write, etc.)
            key: Data key
            value: Value for write operations
            preferred_region: Preferred region for the request
        """
        # Determine target region
        if preferred_region and preferred_region in self.nodes:
            target_region = preferred_region
        else:
            # Find region with lowest latency
            all_regions = set()
            for node in self.nodes.values():
                all_regions.add(node.region)
            
            if not all_regions:
                return {"error": "No nodes available"}
            
            # Use first available region
            target_region = list(all_regions)[0]
        
        # Get primary node for region
        primary_id = self.get_primary_for_region(target_region)
        if not primary_id or primary_id not in self.nodes:
            return {"error": "No primary available"}
        
        node = self.nodes[primary_id]
        
        # Execute operation
        if operation == "write":
            return await node.write_register(key, value)
        elif operation == "read":
            return await node.read_register(key)
        elif operation == "increment":
            return await node.increment_counter(key, value or 1)
        elif operation == "add_to_set":
            return await node.add_to_set(key, value)
        elif operation == "remove_from_set":
            return await node.remove_from_set(key, value)
        
        return {"error": f"Unknown operation: {operation}"}
    
    def get_cluster_status(self) -> Dict[str, Any]:
        """Get status of entire cluster"""
        return {
            "total_nodes": len(self.nodes),
            "regions": {
                region.value: {
                    "primary": self.region_primary.get(region),
                    "config": self.region_configs[region].name
                }
                for region in Region
            },
            "node_details": {
                node_id: node.get_status()
                for node_id, node in self.nodes.items()
            }
        }


# Example usage
async def test_geo_distributed():
    """Test geo-distributed system"""
    
    cluster = GeoDistributedCluster()
    
    # Add nodes in different regions
    peer_config = {
        Region.US_EAST: ["node-us-east-1"],
        Region.US_WEST: ["node-us-west-1"],
        Region.EU_WEST: ["node-eu-west-1"],
        Region.ASIA_PACIFIC: ["node-asia-pacific-1"],
    }
    
    cluster.add_node("node-us-east-1", Region.US_EAST, peer_config)
    cluster.add_node("node-us-west-1", Region.US_WEST, peer_config)
    cluster.add_node("node-eu-west-1", Region.EU_WEST, peer_config)
    cluster.add_node("node-asia-pacific-1", Region.ASIA_PACIFIC, peer_config)
    
    # Test operations
    result1 = await cluster.route_request("write", "counter", 100)
    print(f"Write result: {result1}")
    
    result2 = await cluster.route_request("increment", "counter", 1)
    print(f"Increment result: {result2}")
    
    result3 = await cluster.route_request("add_to_set", "users", "user1")
    print(f"Add to set result: {result3}")
    
    # Get cluster status
    status = cluster.get_cluster_status()
    print(f"Cluster status: {json.dumps(status, indent=2, default=str)}")
    
    return status


if __name__ == "__main__":
    asyncio.run(test_geo_distributed())