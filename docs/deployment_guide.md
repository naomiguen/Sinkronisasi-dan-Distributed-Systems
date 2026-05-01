# Deployment Guide

## Distributed Synchronization System

Panduan ini menjelaskan cara deploy dan menjalankan Distributed Synchronization System menggunakan Docker.

---

## Prerequisites

| Requirement | Version | Notes |
|-------------|---------|-------|
| Docker | 20.10+ | [Install Docker](https://docs.docker.com/get-docker/) |
| Docker Compose | 2.0+ | Included in Docker Desktop |
| Python | 3.11+ | Only for local development |
| Redis | 7.x | Already included in Docker Compose |

### Untuk Local Development (tanpa Docker)
- Redis server harus berjalan di `localhost:6379`
- Python 3.11+ dengan virtual environment

---

## Quick Start

### 1. Clone dan Setup

```bash
# Clone repository (clone into local folder name used in this project)
git clone https://github.com/naomiguen/Sinkronisasi-dan-Distributed-Systems distributed-sync-system
cd distributed-sync-system

# Salin file konfigurasi
cp .env.example .env
```

### 2. Konfigurasi Environment

Edit file `.env` sesuai kebutuhan:

```bash
# Node Configuration
NODE_ID=1
NODE_HOST=0.0.0.0
NODE_PORT=8001

# Cluster Configuration
CLUSTER_NODES=node-1:8001,node-2:8002,node-3:8003

# Redis Configuration
REDIS_HOST=redis
REDIS_PORT=6379

# Monitoring
METRICS_ENABLED=true
LOG_LEVEL=INFO
```

### 3. Jalankan dengan Docker

```bash
# Build dan jalankan semua container
docker-compose up --build

# Atau jalankan di background
docker-compose up -d --build

# Cek status container
docker-compose ps
```

### 4. Verifikasi Installation

```bash
# Cek health semua node
curl http://localhost:8001/health
curl http://localhost:8002/health
curl http://localhost:8003/health

# Cek status cluster (hanya leader yang bisa respond penuh)
curl http://localhost:8001/status
```

---

## Docker Compose Services

### Services yang Berjalan

| Service | Port | Description |
|---------|------|-------------|
| node-1 | 8001 | Node pertama (biasanya leader) |
| node-2 | 8002 | Node kedua (follower) |
| node-3 | 8003 | Node ketiga (follower) |
| redis | 6379 | Distributed state storage |
| prometheus | 9090 | Metrics collection |

### Scaling Nodes

Tambah node ke-4:
```bash
docker-compose up -d --scale node=4
```

Tambah node ke-5:
```bash
docker-compose up -d --scale node=5
```

---

## Local Development (Tanpa Docker)

### 1. Setup Virtual Environment

```bash
# Buat virtual environment
python -m venv venv

# Activate (Linux/Mac)
source venv/bin/activate

# Activate (Windows)
venv\Scripts\activate
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Jalankan Redis

```bash
# Menggunakan Docker
docker run -d -p 6379:6379 redis:7.2-alpine

# Atau install Redis langsung
# Linux: sudo apt-get install redis-server
# Windows: Gunakan WSL atau Docker
```

### 4. Jalankan Nodes

Buka 3 terminal terpisah:

```bash
# Terminal 1 - Node 1
NODE_ID=1 NODE_PORT=8001 python -m src.nodes.base_node

# Terminal 2 - Node 2
NODE_ID=2 NODE_PORT=8002 python -m src.nodes.base_node

# Terminal 3 - Node 3
NODE_ID=3 NODE_PORT=8003 python -m src.nodes.base_node
```

---

## API Usage Examples

### Distributed Lock Manager

```bash
# Acquire exclusive lock
curl -X POST http://localhost:8001/lock/acquire \
  -H "Content-Type: application/json" \
  -d '{
    "resource": "my-resource",
    "lock_type": "exclusive",
    "ttl": 30,
    "client_id": "client-1"
  }'

# Acquire shared lock
curl -X POST http://localhost:8001/lock/acquire \
  -H "Content-Type: application/json" \
  -d '{
    "resource": "my-resource",
    "lock_type": "shared",
    "ttl": 30,
    "client_id": "client-2"
  }'

# Release lock
curl -X POST http://localhost:8001/lock/release \
  -H "Content-Type: application/json" \
  -d '{
    "resource": "my-resource",
    "client_id": "client-1"
  }'

# Check lock status
curl http://localhost:8001/lock/status?resource=my-resource
```

### Distributed Queue

```bash
# Produce message
curl -X POST http://localhost:8001/queue/produce \
  -H "Content-Type: application/json" \
  -d '{
    "queue_name": "orders",
    "payload": {"order_id": "123", "amount": 100},
    "producer_id": "producer-1"
  }'

# Consume message
curl -X POST http://localhost:8001/queue/consume \
  -H "Content-Type: application/json" \
  -d '{
    "queue_name": "orders",
    "consumer_id": "consumer-1",
    "ack_timeout": 30
  }'

# Check queue status
curl http://localhost:8001/queue/status?queue_name=orders
```

### Distributed Cache

```bash
# Set cache value
curl -X PUT http://localhost:8001/cache/set \
  -H "Content-Type: application/json" \
  -d '{
    "key": "user:123",
    "value": {"name": "John", "email": "john@example.com"},
    "ttl": 3600
  }'

# Get cache value
curl http://localhost:8001/cache/get/user:123

# Invalidate cache
curl -X DELETE http://localhost:8001/cache/invalidate/user:123

# Check cache status
curl http://localhost:8001/cache/status
```

---

## Monitoring

### Prometheus Metrics

Akses metrics di:
- Node 1: http://localhost:9091/metrics
- Node 2: http://localhost:9092/metrics
- Node 3: http://localhost:9093/metrics

### Prometheus Dashboard

Buka http://localhost:9090 untuk melihat dashboard Prometheus.

### Available Metrics

| Metric | Description |
|--------|-------------|
| `raft_node_role` | Current role (0=follower, 1=candidate, 2=leader) |
| `raft_current_term` | Current Raft term |
| `lock_requests_total` | Total lock requests |
| `lock_active_count` | Active locks |
| `queue_messages_produced` | Messages produced |
| `queue_messages_consumed` | Messages consumed |
| `cache_hits_total` | Cache hits |
| `cache_misses_total` | Cache misses |

---

## Troubleshooting

### Container tidak start

```bash
# Cek logs
docker-compose logs node-1

# Cek resource usage
docker stats
```

### Node tidak bisa connect ke Redis

```bash
# Cek Redis status
docker-compose exec redis redis-cli ping

# Cek network
docker network ls
docker network inspect distributed-sync-system_cluster-net
```

### Leader tidak terpilih

```bash
# Cek election timeout - mungkin terlalu lama
# Coba restart semua node
docker-compose restart

# Atau rebuild
docker-compose down
docker-compose up --build
```

### Port sudah digunakan

```bash
# Cek port yang digunakan
netstat -ano | findstr "8001"

# Kill process yang menggunakan port
taskkill /PID <PID> /F
```

### Reset everything

```bash
# Stop dan hapus semua container + volumes
docker-compose down -v

# Rebuild dari awal
docker-compose up --build
```

---

## Common Issues

### Q: Bagaimana cara mengetest failure scenario?
A: Matikan satu node dengan `docker-compose stop node-1` dan observe bagaimana cluster bereaksi.

### Q: Apakah bisa dijalankan tanpa Redis?
A: Tidak bisa. Redis adalah komponen wajib untuk distributed state.

### Q: Bagaimana cara menambah node baru?
A: Edit docker-compose.yml untuk menambah service node-N, atau gunakan `--scale`.

### Q: Kenapa lock tidak bisa diperoleh?
A: Pastikan request dikirim ke leader. Cek status dengan `/lock/status`.

---

## Performance Tuning

### Untuk Production

1. Tingkatkan resource limits di docker-compose.yml
2. Gunakan Redis cluster untuk high availability
3. Aktifkan TLS untuk komunikasi antar node
4. Setup monitoring dengan Grafana

### Untuk Development

1. Kurangi election timeout di config.py untuk faster leader election
2. Disable metrics jika tidak diperlukan
3. Gunakan volumes untuk hot-reload

---

## Security Considerations

- Default `.env` tidak memiliki password - tambahkan untuk production
- Gunakan network isolation dengan custom network
- Enable TLS untuk inter-node communication
- Implement RBAC jika diperlukan

---

## Next Steps

1. Jalankan semua test: `pytest tests/`
2. Load testing: `locust -f tests/performance/locustfile.py`
3. Lihat metrics di Prometheus
4. Buat video demo untuk submission