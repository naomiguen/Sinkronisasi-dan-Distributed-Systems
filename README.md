# Distributed Synchronization System

Implementasi sistem sinkronisasi terdistribusi untuk mata kuliah **Sistem Parallel dan Terdistribusi**.

## Deskripsi

Sistem ini mensimulasikan skenario real-world dari distributed systems dengan 4 komponen utama:

| Komponen | Algoritma |
|---|---|
| Distributed Lock Manager | Raft Consensus |
| Distributed Queue | Consistent Hashing |
| Cache Coherence | Protokol MESI |
| Containerization | Docker & Compose |

## Arsitektur

```
┌─────────────────────────────────────────────────┐
│                  Client / API                   │
└──────────────┬───────────────────────────────────┘
               │
    ┌──────────▼──────────┐
    │   Node 1 (Leader)   │◄──── Raft Heartbeat
    │   Port: 8001        │────► Node 2 (Follower) :8002
    │                     │────► Node 3 (Follower) :8003
    └──────────┬──────────┘
               │
    ┌──────────▼──────────┐
    │       Redis         │  ← Shared distributed state
    │   Port: 6379        │
    └─────────────────────┘
```

## Prasyarat

- Python 3.11+
- Docker & Docker Compose
- Redis 7.x (jika tidak pakai Docker)

## Cara Menjalankan

### Dengan Docker (Direkomendasikan)

```bash
# 1. Clone dan masuk ke direktori
# clone repository (will clone into local folder `distributed-sync-system`)
git clone https://github.com/naomiguen/Sinkronisasi-dan-Distributed-Systems distributed-sync-system
cd distributed-sync-system

# 2. Salin file konfigurasi
cp .env.example .env

# 3. Build dan jalankan semua node
docker-compose up --build

# 4. Cek status cluster
curl http://localhost:8001/health
curl http://localhost:8002/health
curl http://localhost:8003/health
```

### Tanpa Docker (Development Lokal)

```bash
# 1. Buat virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# atau: venv\Scripts\activate  # Windows

# 2. Install dependencies
pip install -r requirements.txt

# 3. Pastikan Redis berjalan
redis-server

# 4. Jalankan 3 node di terminal terpisah
NODE_ID=1 NODE_PORT=8001 python -m src.nodes.base_node
NODE_ID=2 NODE_PORT=8002 python -m src.nodes.base_node
NODE_ID=3 NODE_PORT=8003 python -m src.nodes.base_node
```

## API Endpoints

### Lock Manager

```bash
# Minta exclusive lock
POST http://localhost:8001/lock/acquire
{
  "resource": "my-resource",
  "lock_type": "exclusive",
  "ttl": 30,
  "client_id": "client-1"
}

# Lepas lock
POST http://localhost:8001/lock/release
{
  "resource": "my-resource",
  "client_id": "client-1"
}

# Lihat semua lock aktif
GET http://localhost:8001/lock/status
```

### Queue System

```bash
# Kirim pesan ke queue
POST http://localhost:8001/queue/produce
{
  "queue": "my-queue",
  "message": "Hello, World!",
  "producer_id": "prod-1"
}

# Ambil pesan dari queue
POST http://localhost:8001/queue/consume
{
  "queue": "my-queue",
  "consumer_id": "cons-1"
}
```

### Cache

```bash
# Set nilai ke cache
PUT http://localhost:8001/cache/set
{
  "key": "user:123",
  "value": {"name": "Budi"},
  "ttl": 60
}

# Get nilai dari cache
GET http://localhost:8001/cache/get/user:123

# Invalidasi cache
DELETE http://localhost:8001/cache/invalidate/user:123
```

## Menjalankan Tests

```bash
# Unit tests
pytest tests/unit/ -v

# Integration tests (perlu Redis)
pytest tests/integration/ -v

# Semua tests dengan coverage
pytest --cov=src --cov-report=html
```

## Monitoring

Setelah sistem berjalan, akses:
- **Node 1 Metrics**: http://localhost:9091/metrics
- **Node 2 Metrics**: http://localhost:9092/metrics
- **Node 3 Metrics**: http://localhost:9093/metrics

Jalankan dengan Prometheus:
```bash
docker-compose --profile monitoring up
```

## Struktur Proyek

```
distributed-sync-system/
├── src/
│   ├── nodes/
│   │   ├── base_node.py       ← HTTP server + entry point
│   │   ├── lock_manager.py    ← Distributed Lock (Raft)
│   │   ├── queue_node.py      ← Distributed Queue
│   │   └── cache_node.py      ← Cache Coherence (MESI)
│   ├── consensus/
│   │   └── raft.py            ← Implementasi Raft Consensus
│   ├── communication/
│   │   ├── message_passing.py ← RPC antar node
│   │   └── failure_detector.py← Deteksi node failure
│   └── utils/
│       ├── config.py          ← Konfigurasi terpusat
│       └── metrics.py         ← Prometheus metrics
├── tests/
│   ├── unit/                  ← Unit tests per komponen
│   ├── integration/           ← Integration tests
│   └── performance/           ← Load tests (locust)
├── docker/
│   ├── Dockerfile.node        ← Image untuk node
│   ├── redis.conf             ← Konfigurasi Redis
│   └── prometheus.yml         ← Konfigurasi Prometheus
├── docs/
│   ├── architecture.md        ← Dokumentasi arsitektur
│   └── api_spec.yaml          ← OpenAPI specification
├── benchmarks/
│   └── load_test_scenarios.py ← Skenario load testing
├── docker-compose.yml
├── requirements.txt
├── .env.example
└── README.md
```

## Referensi

- [Raft Consensus Algorithm](https://raft.github.io/)
- [Distributed Systems - Maarten van Steen](https://www.distributed-systems.net/)
- [Redis Distributed Locks](https://redis.io/docs/latest/develop/clients/patterns/distributed-locks/)
- [Distributed Systems for Fun and Profit](https://book.mixu.net/distsys/)
- [Designing Data-Intensive Applications](https://dataintensive.net/)

## Video Demo

> Link YouTube akan ditambahkan setelah recording selesai.

---
