import uuid
import random
from locust import HttpUser, task, between, events
from locust.runners import MasterRunner


# USER SIMULASI: Lock Manager

class LockUser(HttpUser):
    """Simulasi client yang melakukan acquire dan release lock."""

    wait_time = between(0.5, 2.0)
    weight = 3

    def on_start(self):
        self.client_id = f"load-client-{uuid.uuid4().hex[:8]}"
        self.held_locks = []

    @task(5)
    def acquire_exclusive_lock(self):
        """Minta exclusive lock untuk resource acak."""
        resource = f"resource-{random.randint(1, 10)}"
        with self.client.post(
            "/lock/acquire",
            json={
                "resource": resource,
                "lock_type": "exclusive",
                "client_id": self.client_id,
                "ttl": 10,
            },
            catch_response=True,
            name="lock/acquire [exclusive]",
        ) as resp:
            if resp.status_code == 200:
                data = resp.json()
                if data.get("status") == "granted":
                    self.held_locks.append(resource)
                    resp.success()
                else:
                    resp.success()
            elif resp.status_code == 307:
                resp.success()
            else:
                resp.failure(f"HTTP {resp.status_code}")

    @task(3)
    def acquire_shared_lock(self):
        """Minta shared lock untuk resource acak."""
        resource = f"shared-{random.randint(1, 5)}"
        with self.client.post(
            "/lock/acquire",
            json={
                "resource": resource,
                "lock_type": "shared",
                "client_id": self.client_id,
                "ttl": 15,
            },
            catch_response=True,
            name="lock/acquire [shared]",
        ) as resp:
            if resp.status_code in [200, 307]:
                resp.success()
            else:
                resp.failure(f"HTTP {resp.status_code}")

    @task(4)
    def release_lock(self):
        """Lepas lock yang sedang dipegang."""
        if not self.held_locks:
            return
        resource = self.held_locks.pop(0)
        with self.client.post(
            "/lock/release",
            json={"resource": resource, "client_id": self.client_id},
            catch_response=True,
            name="lock/release",
        ) as resp:
            if resp.status_code in [200, 307]:
                resp.success()
            else:
                resp.failure(f"HTTP {resp.status_code}")

    @task(1)
    def check_lock_status(self):
        """Lihat semua lock aktif."""
        self.client.get("/lock/status", name="lock/status")


# USER SIMULASI: Queue

class QueueUser(HttpUser):
    """Simulasi producer dan consumer yang menggunakan distributed queue."""

    wait_time = between(0.3, 1.5)
    weight = 2

    QUEUES = ["orders", "emails", "notifications", "payments", "inventory"]

    def on_start(self):
        self.producer_id = f"producer-{uuid.uuid4().hex[:8]}"
        self.consumer_id = f"consumer-{uuid.uuid4().hex[:8]}"
        self.in_flight_messages = []

    @task(4)
    def produce_message(self):
        """Kirim pesan ke queue acak."""
        queue = random.choice(self.QUEUES)
        with self.client.post(
            "/queue/produce",
            json={
                "queue": queue,
                "message": {
                    "id": str(uuid.uuid4()),
                    "type": random.choice(["create", "update", "delete"]),
                    "payload": {"value": random.randint(1, 1000)},
                },
                "producer_id": self.producer_id,
            },
            catch_response=True,
            name="queue/produce",
        ) as resp:
            if resp.status_code in [200, 307]:
                resp.success()
            else:
                resp.failure(f"HTTP {resp.status_code}")

    @task(3)
    def consume_message(self):
        """Ambil pesan dari queue acak."""
        queue = random.choice(self.QUEUES)
        with self.client.post(
            "/queue/consume",
            json={
                "queue": queue,
                "consumer_id": self.consumer_id,
                "ack_timeout": 30,
            },
            catch_response=True,
            name="queue/consume",
        ) as resp:
            if resp.status_code == 200:
                data = resp.json()
                if data.get("success") and data.get("message"):
                    msg_id = data["message"]["message_id"]
                    self.in_flight_messages.append(msg_id)
                resp.success()
            else:
                resp.failure(f"HTTP {resp.status_code}")

    @task(2)
    def acknowledge_message(self):
        """ACK pesan yang sedang in-flight."""
        if not self.in_flight_messages:
            return
        msg_id = self.in_flight_messages.pop(0)
        with self.client.post(
            "/queue/ack",
            json={"message_id": msg_id, "consumer_id": self.consumer_id},
            catch_response=True,
            name="queue/ack",
        ) as resp:
            if resp.status_code == 200:
                resp.success()
            else:
                resp.failure(f"HTTP {resp.status_code}")

    @task(1)
    def check_queue_status(self):
        """Lihat status semua queue."""
        self.client.get("/queue/status", name="queue/status")


# USER SIMULASI: Cache

class CacheUser(HttpUser):
    """Simulasi client dengan pola akses cache yang realistis (80% read, 20% write)."""

    wait_time = between(0.1, 0.5)
    weight = 5

    KEYS = [f"user:{i}" for i in range(1, 51)] + \
           [f"product:{i}" for i in range(1, 21)] + \
           [f"config:{k}" for k in ["timeout", "retries", "debug", "version"]]

    def on_start(self):
        self._seed_cache()

    def _seed_cache(self):
        """Isi beberapa key awal ke cache sebelum mulai load test."""
        for i in range(5):
            key = random.choice(self.KEYS)
            self.client.put(
                "/cache/set",
                json={"key": key, "value": {"seeded": True, "index": i}, "ttl": 300},
                name="cache/set [seed]",
            )

    @task(8)
    def cache_get(self):
        """Baca dari cache (mayoritas operasi)."""
        key = random.choice(self.KEYS)
        self.client.get(f"/cache/get/{key}", name="cache/get")

    @task(2)
    def cache_set(self):
        """Tulis ke cache."""
        key = random.choice(self.KEYS)
        with self.client.put(
            "/cache/set",
            json={
                "key": key,
                "value": {
                    "name": f"entity-{random.randint(1, 1000)}",
                    "score": random.uniform(0, 100),
                    "active": random.choice([True, False]),
                },
                "ttl": random.choice([60, 120, 300, None]),
            },
            catch_response=True,
            name="cache/set",
        ) as resp:
            if resp.status_code in [200, 307]:
                resp.success()
            else:
                resp.failure(f"HTTP {resp.status_code}")

    @task(1)
    def cache_invalidate(self):
        """Invalidasi cache entry."""
        key = random.choice(self.KEYS)
        with self.client.delete(
            f"/cache/invalidate/{key}",
            catch_response=True,
            name="cache/invalidate",
        ) as resp:
            if resp.status_code in [200, 307]:
                resp.success()
            else:
                resp.failure(f"HTTP {resp.status_code}")

    @task(1)
    def cache_status(self):
        """Lihat statistik cache."""
        self.client.get("/cache/status", name="cache/status")


# USER SIMULASI: Health Check (monitoring)

class HealthCheckUser(HttpUser):
    """Simulasi monitoring yang terus-menerus cek health node."""

    wait_time = between(1.0, 3.0)
    weight = 1

    @task(3)
    def health(self):
        self.client.get("/health", name="/health")

    @task(1)
    def status(self):
        self.client.get("/status", name="/status")


# EVENT HOOKS - untuk custom reporting

@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    print("\n" + "="*60)
    print("Load Test Distributed Sync System dimulai")
    print(f"Target host: {environment.host}")
    print("="*60 + "\n")


@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    stats = environment.stats
    print("\n" + "="*60)
    print("HASIL LOAD TEST")
    print("="*60)
    for name, stat in stats.entries.items():
        if stat.num_requests > 0:
            print(
                f"{name[1]:40s} | "
                f"req={stat.num_requests:6d} | "
                f"fail={stat.num_failures:4d} | "
                f"avg={stat.avg_response_time:7.1f}ms | "
                f"p95={stat.get_response_time_percentile(0.95):7.1f}ms"
            )
    print("="*60 + "\n")