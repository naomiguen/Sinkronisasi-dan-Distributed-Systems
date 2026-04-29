import time
from prometheus_client import (
    Counter, Gauge, Histogram, start_http_server, REGISTRY
)
from src.utils.config import config


class Metrics:
    def __init__(self):
        
        # Raft / Node State
        
        self.node_role = Gauge(
            "raft_node_role",
            "Role node saat ini: 0=follower, 1=candidate, 2=leader",
            ["node_id"]
        )
        self.raft_term = Gauge(
            "raft_current_term",
            "Raft term saat ini",
            ["node_id"]
        )
        self.election_total = Counter(
            "raft_elections_total",
            "Total jumlah leader election yang terjadi",
            ["node_id"]
        )
        self.heartbeat_sent = Counter(
            "raft_heartbeat_sent_total",
            "Total heartbeat yang dikirim (hanya leader)",
            ["node_id"]
        )

        
        # Distributed Lock Manager
        
        self.lock_requests_total = Counter(
            "lock_requests_total",
            "Total request lock",
            ["node_id", "lock_type", "result"]  # result: granted/denied/timeout
        )
        self.active_locks = Gauge(
            "lock_active_count",
            "Jumlah lock yang aktif saat ini",
            ["node_id"]
        )
        self.lock_latency = Histogram(
            "lock_operation_duration_seconds",
            "Latency operasi lock dalam detik",
            ["node_id", "operation"],  # operation: acquire/release
            buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0]
        )
        self.deadlock_detected = Counter(
            "lock_deadlock_detected_total",
            "Total deadlock yang terdeteksi",
            ["node_id"]
        )

        
        # Distributed Queue
        
        self.queue_messages_produced = Counter(
            "queue_messages_produced_total",
            "Total pesan yang diproduksi",
            ["node_id", "queue_name"]
        )
        self.queue_messages_consumed = Counter(
            "queue_messages_consumed_total",
            "Total pesan yang dikonsumsi",
            ["node_id", "queue_name"]
        )
        self.queue_depth = Gauge(
            "queue_depth",
            "Jumlah pesan yang menunggu di queue",
            ["node_id", "queue_name"]
        )

        
        # Cache Coherence
        
        self.cache_hits = Counter(
            "cache_hits_total",
            "Total cache hit",
            ["node_id"]
        )
        self.cache_misses = Counter(
            "cache_misses_total",
            "Total cache miss",
            ["node_id"]
        )
        self.cache_invalidations = Counter(
            "cache_invalidations_total",
            "Total cache invalidation",
            ["node_id"]
        )
        self.cache_size = Gauge(
            "cache_current_size",
            "Jumlah entry dalam cache saat ini",
            ["node_id"]
        )

        self.node_id = str(config.node.node_id)
        self._started = False

    def start_server(self):
        """Jalankan HTTP server untuk Prometheus scraping."""
        if not self._started and config.metrics_enabled:
            start_http_server(config.metrics_port)
            self._started = True
            print(f"Metrics server running on port {config.metrics_port}")

    # Helper methods agar pemanggilan lebih ringkas
    def record_lock_latency(self, duration: float, operation: str = "acquire"):
        self.lock_latency.labels(
            node_id=self.node_id, operation=operation
        ).observe(duration)

    def lock_granted(self, lock_type: str = "exclusive"):
        self.lock_requests_total.labels(
            node_id=self.node_id, lock_type=lock_type, result="granted"
        ).inc()

    def lock_denied(self, lock_type: str = "exclusive"):
        self.lock_requests_total.labels(
            node_id=self.node_id, lock_type=lock_type, result="denied"
        ).inc()

    def set_role(self, role: str):
        """role: 'follower', 'candidate', atau 'leader'"""
        role_map = {"follower": 0, "candidate": 1, "leader": 2}
        self.node_role.labels(node_id=self.node_id).set(role_map.get(role, 0))

    def record_cache_hit(self):
        self.cache_hits.labels(node_id=self.node_id).inc()

    def record_cache_miss(self):
        self.cache_misses.labels(node_id=self.node_id).inc()


# Singleton
metrics = Metrics()