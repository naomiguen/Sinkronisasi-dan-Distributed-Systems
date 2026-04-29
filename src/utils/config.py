
import os
from dataclasses import dataclass, field
from typing import List
from dotenv import load_dotenv

# Load .env file otomatis saat module ini diimport
load_dotenv()


@dataclass
class NodeConfig:
    """Konfigurasi identitas node ini."""
    node_id: int = field(default_factory=lambda: int(os.getenv("NODE_ID", "1")))
    host: str = field(default_factory=lambda: os.getenv("NODE_HOST", "0.0.0.0"))
    port: int = field(default_factory=lambda: int(os.getenv("NODE_PORT", "8001")))

    @property
    def address(self) -> str:
        return f"{self.host}:{self.port}"


@dataclass
class ClusterConfig:
    """Konfigurasi cluster - daftar semua node."""
    nodes_raw: str = field(
        default_factory=lambda: os.getenv(
            "CLUSTER_NODES", "localhost:8001,localhost:8002,localhost:8003"
        )
    )

    @property
    def nodes(self) -> List[dict]:
        """
        Parse CLUSTER_NODES menjadi list of dict.
        
        Returns:
            [{"host": "localhost", "port": 8001}, ...]
        """
        result = []
        for entry in self.nodes_raw.split(","):
            entry = entry.strip()
            if ":" in entry:
                host, port = entry.split(":", 1)
                result.append({"host": host.strip(), "port": int(port.strip())})
        return result

    @property
    def cluster_size(self) -> int:
        return len(self.nodes)

    @property
    def quorum(self) -> int:
        """Jumlah node minimum untuk mencapai majority (quorum)."""
        return (self.cluster_size // 2) + 1


@dataclass
class RedisConfig:
    """Konfigurasi koneksi Redis."""
    host: str = field(default_factory=lambda: os.getenv("REDIS_HOST", "localhost"))
    port: int = field(default_factory=lambda: int(os.getenv("REDIS_PORT", "6379")))
    db: int = field(default_factory=lambda: int(os.getenv("REDIS_DB", "0")))
    password: str = field(default_factory=lambda: os.getenv("REDIS_PASSWORD", ""))
    max_connections: int = field(
        default_factory=lambda: int(os.getenv("REDIS_MAX_CONNECTIONS", "20"))
    )

    @property
    def url(self) -> str:
        if self.password:
            return f"redis://:{self.password}@{self.host}:{self.port}/{self.db}"
        return f"redis://{self.host}:{self.port}/{self.db}"


@dataclass
class RaftConfig:
    """Parameter algoritma Raft Consensus."""
    election_timeout_min: int = field(
        default_factory=lambda: int(os.getenv("RAFT_ELECTION_TIMEOUT_MIN", "150"))
    )
    election_timeout_max: int = field(
        default_factory=lambda: int(os.getenv("RAFT_ELECTION_TIMEOUT_MAX", "300"))
    )
    heartbeat_interval: int = field(
        default_factory=lambda: int(os.getenv("RAFT_HEARTBEAT_INTERVAL", "50"))
    )
    rpc_timeout: int = field(
        default_factory=lambda: int(os.getenv("RAFT_RPC_TIMEOUT", "100"))
    )


@dataclass
class LockConfig:
    """Konfigurasi Distributed Lock Manager."""
    default_ttl: int = field(
        default_factory=lambda: int(os.getenv("LOCK_DEFAULT_TTL", "30"))
    )
    max_ttl: int = field(
        default_factory=lambda: int(os.getenv("LOCK_MAX_TTL", "300"))
    )
    retry_interval: int = field(
        default_factory=lambda: int(os.getenv("LOCK_RETRY_INTERVAL", "100"))
    )
    max_retries: int = field(
        default_factory=lambda: int(os.getenv("LOCK_MAX_RETRIES", "10"))
    )


@dataclass
class CacheConfig:
    """Konfigurasi Distributed Cache Coherence."""
    protocol: str = field(default_factory=lambda: os.getenv("CACHE_PROTOCOL", "MESI"))
    max_size: int = field(
        default_factory=lambda: int(os.getenv("CACHE_MAX_SIZE", "1000"))
    )
    replacement_policy: str = field(
        default_factory=lambda: os.getenv("CACHE_REPLACEMENT_POLICY", "LRU")
    )
    invalidation_timeout: int = field(
        default_factory=lambda: int(os.getenv("CACHE_INVALIDATION_TIMEOUT", "5"))
    )


@dataclass
class Config:
    """
    Root config object - kumpulan semua konfigurasi sistem.
    
    Semua modul mengimport ini:
        from src.utils.config import config
    """
    node: NodeConfig = field(default_factory=NodeConfig)
    cluster: ClusterConfig = field(default_factory=ClusterConfig)
    redis: RedisConfig = field(default_factory=RedisConfig)
    raft: RaftConfig = field(default_factory=RaftConfig)
    lock: LockConfig = field(default_factory=LockConfig)
    cache: CacheConfig = field(default_factory=CacheConfig)

    log_level: str = field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO"))
    environment: str = field(
        default_factory=lambda: os.getenv("ENVIRONMENT", "development")
    )
    metrics_enabled: bool = field(
        default_factory=lambda: os.getenv("METRICS_ENABLED", "true").lower() == "true"
    )
    metrics_port: int = field(
        default_factory=lambda: int(os.getenv("METRICS_PORT", "9090"))
    )

    def is_development(self) -> bool:
        return self.environment == "development"

    def is_testing(self) -> bool:
        return self.environment == "testing"


# Singleton - semua modul pakai instance yang sama
config = Config()