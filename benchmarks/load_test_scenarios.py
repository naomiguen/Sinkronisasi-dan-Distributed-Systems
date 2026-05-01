import asyncio
import json
import random
import time
import statistics
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Callable
from concurrent.futures import ThreadPoolExecutor
import threading

import aiohttp


# CONFIGURATION
BASE_URL = "http://localhost:8001"
NUM_NODES = 3
TEST_DURATION = 60  # detik

# TEST RESULTS DATA CLASS
@dataclass
class TestResult:
    """Hasil dari satu skenario test."""
    name: str
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    min_latency: float = float('inf')
    max_latency: float = 0
    latencies: List[float] = field(default_factory=list)
    throughput: float = 0  # requests per second
    avg_latency: float = 0
    p50_latency: float = 0
    p95_latency: float = 0
    p99_latency: float = 0
    
    def calculate_stats(self):
        """Hitung statistik dari latencies yang dikumpulkan."""
        if not self.latencies:
            return
            
        self.total_requests = len(self.latencies)
        self.successful_requests = self.total_requests - self.failed_requests
        
        sorted_latencies = sorted(self.latencies)
        self.min_latency = min(sorted_latencies)
        self.max_latency = max(sorted_latencies)
        self.avg_latency = statistics.mean(sorted_latencies)
        
        n = len(sorted_latencies)
        self.p50_latency = sorted_latencies[int(n * 0.50)]
        self.p95_latency = sorted_latencies[int(n * 0.95)]
        self.p99_latency = sorted_latencies[int(n * 0.99)]
        
        # Throughput = requests / waktu
        if self.total_requests > 0:
            self.throughput = self.total_requests / TEST_DURATION
    
    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "total_requests": self.total_requests,
            "successful_requests": self.successful_requests,
            "failed_requests": self.failed_requests,
            "throughput": round(self.throughput, 2),
            "min_latency_ms": round(self.min_latency * 1000, 2),
            "max_latency_ms": round(self.max_latency * 1000, 2),
            "avg_latency_ms": round(self.avg_latency * 1000, 2),
            "p50_latency_ms": round(self.p50_latency * 1000, 2),
            "p95_latency_ms": round(self.p95_latency * 1000, 2),
            "p99_latency_ms": round(self.p99_latency * 1000, 2),
        }



# HTTP CLIENT
class HttpClient:
    """Async HTTP client untuk load testing."""
    
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.session: Optional[aiohttp.ClientSession] = None
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, *args):
        if self.session:
            await self.session.close()
    
    async def post(self, path: str, json_data: dict, timeout: int = 30) -> tuple:
        """Kirim POST request, return (success, latency)."""
        start = time.perf_counter()
        try:
            async with self.session.post(
                f"{self.base_url}{path}",
                json=json_data,
                timeout=aiohttp.ClientTimeout(total=timeout)
            ) as response:
                latency = time.perf_counter() - start
                success = response.status < 400
                return success, latency
        except Exception as e:
            latency = time.perf_counter() - start
            return False, latency
    
    async def get(self, path: str, timeout: int = 30) -> tuple:
        """Kirim GET request, return (success, latency)."""
        start = time.perf_counter()
        try:
            async with self.session.get(
                f"{self.base_url}{path}",
                timeout=aiohttp.ClientTimeout(total=timeout)
            ) as response:
                latency = time.perf_counter() - start
                success = response.status < 400
                return success, latency
        except Exception as e:
            latency = time.perf_counter() - start
            return False, latency
    
    async def put(self, path: str, json_data: dict, timeout: int = 30) -> tuple:
        """Kirim PUT request, return (success, latency)."""
        start = time.perf_counter()
        try:
            async with self.session.put(
                f"{self.base_url}{path}",
                json=json_data,
                timeout=aiohttp.ClientTimeout(total=timeout)
            ) as response:
                latency = time.perf_counter() - start
                success = response.status < 400
                return success, latency
        except Exception as e:
            latency = time.perf_counter() - start
            return False, latency
    
    async def delete(self, path: str, timeout: int = 30) -> tuple:
        """Kirim DELETE request, return (success, latency)."""
        start = time.perf_counter()
        try:
            async with self.session.delete(
                f"{self.base_url}{path}",
                timeout=aiohttp.ClientTimeout(total=timeout)
            ) as response:
                latency = time.perf_counter() - start
                success = response.status < 400
                return success, latency
        except Exception as e:
            latency = time.perf_counter() - start
            return False, latency


# TEST SCENARIOS
async def test_lock_acquire_release(client: HttpClient, num_requests: int, 
                                     concurrent: int) -> TestResult:
    """
    Skenario 1: Lock Acquire & Release
    Mengukur throughput dan latency untuk operasi lock.
    """
    result = TestResult(name="Lock Acquire/Release")
    
    async def single_lock_operation():
        resource = f"resource-{random.randint(1, 100)}"
        client_id = f"client-{random.randint(1, 1000)}"
        
        # Acquire lock
        success, latency = await client.post("/lock/acquire", {
            "resource": resource,
            "lock_type": "exclusive",
            "ttl": 30,
            "client_id": client_id
        })
        result.latencies.append(latency)
        if not success:
            result.failed_requests += 1
        
        # Release lock (jika acquire berhasil)
        if success:
            await client.post("/lock/release", {
                "resource": resource,
                "client_id": client_id
            })
    
    # Run concurrent operations
    semaphore = asyncio.Semaphore(concurrent)
    
    async def bounded_operation():
        async with semaphore:
            await single_lock_operation()
    
    tasks = [bounded_operation() for _ in range(num_requests)]
    await asyncio.gather(*tasks)
    
    result.calculate_stats()
    return result


async def test_shared_lock(client: HttpClient, num_requests: int,
                           concurrent: int) -> TestResult:
    """
    Skenario 2: Shared Lock (Multiple Readers)
    Menguji kemampuan shared locks dengan banyak reader.
    """
    result = TestResult(name="Shared Lock (Multiple Readers)")
    
    resource = "shared-resource"
    
    async def acquire_shared_lock():
        client_id = f"reader-{random.randint(1, 1000)}"
        success, latency = await client.post("/lock/acquire", {
            "resource": resource,
            "lock_type": "shared",
            "ttl": 10,
            "client_id": client_id
        })
        result.latencies.append(latency)
        if not success:
            result.failed_requests += 1
    
    semaphore = asyncio.Semaphore(concurrent)
    
    async def bounded_operation():
        async with semaphore:
            await acquire_shared_lock()
    
    tasks = [bounded_operation() for _ in range(num_requests)]
    await asyncio.gather(*tasks)
    
    result.calculate_stats()
    return result


async def test_queue_produce_consume(client: HttpClient, num_requests: int,
                                      concurrent: int) -> TestResult:
    """
    Skenario 3: Queue Produce & Consume
    Mengukur throughput untuk operasi queue.
    """
    result = TestResult(name="Queue Produce/Consume")
    
    async def produce_consume_operation():
        queue_name = f"queue-{random.randint(1, 10)}"
        producer_id = f"producer-{random.randint(1, 100)}"
        
        # Produce message
        success, latency = await client.post("/queue/produce", {
            "queue_name": queue_name,
            "payload": {"data": f"message-{random.randint(1, 10000)}"},
            "producer_id": producer_id
        })
        result.latencies.append(latency)
        if not success:
            result.failed_requests += 1
        
        # Consume message
        if success:
            await client.post("/queue/consume", {
                "queue_name": queue_name,
                "consumer_id": f"consumer-{random.randint(1, 100)}",
                "ack_timeout": 30
            })
    
    semaphore = asyncio.Semaphore(concurrent)
    
    async def bounded_operation():
        async with semaphore:
            await produce_consume_operation()
    
    tasks = [bounded_operation() for _ in range(num_requests)]
    await asyncio.gather(*tasks)
    
    result.calculate_stats()
    return result


async def test_cache_operations(client: HttpClient, num_requests: int,
                                 concurrent: int) -> TestResult:
    """
    Skenario 4: Cache Get/Set Operations
    Mengukur throughput untuk operasi cache.
    """
    result = TestResult(name="Cache Get/Set")
    
    async def cache_operation():
        key = f"key-{random.randint(1, 1000)}"
        
        # Set cache
        success, latency = await client.put("/cache/set", {
            "key": key,
            "value": {"data": f"value-{random.randint(1, 10000)}"},
            "ttl": 3600
        })
        result.latencies.append(latency)
        if not success:
            result.failed_requests += 1
        
        # Get cache
        if success:
            await client.get(f"/cache/get/{key}")
    
    semaphore = asyncio.Semaphore(concurrent)
    
    async def bounded_operation():
        async with semaphore:
            await cache_operation()
    
    tasks = [bounded_operation() for _ in range(num_requests)]
    await asyncio.gather(*tasks)
    
    result.calculate_stats()
    return result


async def test_mixed_operations(client: HttpClient, num_requests: int,
                                 concurrent: int) -> TestResult:
    """
    Skenario 5: Mixed Operations
    Menguji performa dengan kombinasi semua operasi.
    """
    result = TestResult(name="Mixed Operations (Lock + Queue + Cache)")
    
    operations = [
        ("lock", lambda: client.post("/lock/acquire", {
            "resource": f"resource-{random.randint(1, 50)}",
            "lock_type": "exclusive",
            "ttl": 5,
            "client_id": f"client-{random.randint(1, 500)}"
        })),
        ("queue", lambda: client.post("/queue/produce", {
            "queue_name": f"queue-{random.randint(1, 5)}",
            "payload": {"data": f"msg-{random.randint(1, 1000)}"},
            "producer_id": f"prod-{random.randint(1, 50)}"
        })),
        ("cache", lambda: client.put("/cache/set", {
            "key": f"key-{random.randint(1, 500)}",
            "value": {"data": f"val-{random.randint(1, 1000)}"},
            "ttl": 1800
        })),
    ]
    
    async def random_operation():
        op_type, op_func = random.choice(operations)
        success, latency = await op_func()
        result.latencies.append(latency)
        if not success:
            result.failed_requests += 1
    
    semaphore = asyncio.Semaphore(concurrent)
    
    async def bounded_operation():
        async with semaphore:
            await random_operation()
    
    tasks = [bounded_operation() for _ in range(num_requests)]
    await asyncio.gather(*tasks)
    
    result.calculate_stats()
    return result


async def test_scalability(client: HttpClient, num_nodes_list: List[int]) -> TestResult:
    """
    Skenario 6: Scalability Test
    Mengukur performa seiring penambahan node.
    """
    result = TestResult(name="Scalability Test")
    
    # Test dengan 1, 2, 3 node
    for node_count in num_nodes_list:
        print(f"  Testing with {node_count} nodes...")
        
        # Simulasi dengan concurrent requests
        num_requests = 100
        concurrent = 20
        
        async with HttpClient(f"http://localhost:{8000 + node_count}") as c:
            tasks = [
                c.post("/lock/acquire", {
                    "resource": f"resource-{i}",
                    "lock_type": "exclusive",
                    "ttl": 5,
                    "client_id": f"client-{i}"
                })
                for i in range(num_requests)
            ]
            
            start = time.perf_counter()
            responses = await asyncio.gather(*tasks, return_exceptions=True)
            duration = time.perf_counter() - start
            
            successful = sum(1 for s, _ in responses if s and not isinstance(s, Exception))
            throughput = successful / duration if duration > 0 else 0
            
            print(f"    {node_count} nodes: {throughput:.2f} req/s")
    
    result.calculate_stats()
    return result


async def test_failure_recovery(client: HttpClient) -> TestResult:
    """
    Skenario 7: Failure Recovery
    Menguji ketahanan sistem saat node gagal.
    """
    result = TestResult(name="Failure Recovery")
    
    # Baseline - semua node hidup
    print("  Testing baseline (all nodes alive)...")
    baseline_tasks = [
        client.post("/lock/acquire", {
            "resource": f"resource-{i}",
            "lock_type": "exclusive",
            "ttl": 5,
            "client_id": f"client-{i}"
        })
        for i in range(50)
    ]
    
    start = time.perf_counter()
    await asyncio.gather(*baseline_tasks, return_exceptions=True)
    baseline_duration = time.perf_counter() - start
    baseline_throughput = 50 / baseline_duration
    
    print(f"    Baseline: {baseline_throughput:.2f} req/s")
    
    # Catat bahwa failure testing memerlukan simulasi manual
    # (matikan node dengan docker-compose stop node-X)
    print("  Note: Untuk full failure test, matikan node dengan:")
    print("    docker-compose stop node-2")
    print("  Kemudian jalankan test ini lagi untuk melihat impact")
    
    result.throughput = baseline_throughput
    return result

# BENCHMARK RUNNER
async def run_benchmark(scenario: Callable, **kwargs) -> TestResult:
    """Jalankan satu skenario benchmark."""
    print(f"\n{'='*60}")
    print(f"Running: {scenario.__name__}")
    print(f"{'='*60}")
    
    async with HttpClient(BASE_URL) as client:
        result = await scenario(client, **kwargs)
    
    return result


async def run_all_benchmarks():
    """Jalankan semua skenario benchmark."""
    print("="*60)
    print("DISTRIBUTED SYNCHRONIZATION SYSTEM - BENCHMARK SUITE")
    print("="*60)
    print(f"Base URL: {BASE_URL}")
    print(f"Test Duration: {TEST_DURATION}s per scenario")
    print(f"Nodes: {NUM_NODES}")
    
    results: List[TestResult] = []
    
    # Test 1: Lock Acquire/Release
    result = await run_benchmark(
        test_lock_acquire_release,
        num_requests=1000,
        concurrent=50
    )
    results.append(result)
    
    # Test 2: Shared Lock
    result = await run_benchmark(
        test_shared_lock,
        num_requests=1000,
        concurrent=50
    )
    results.append(result)
    
    # Test 3: Queue Operations
    result = await run_benchmark(
        test_queue_produce_consume,
        num_requests=1000,
        concurrent=50
    )
    results.append(result)
    
    # Test 4: Cache Operations
    result = await run_benchmark(
        test_cache_operations,
        num_requests=1000,
        concurrent=50
    )
    results.append(result)
    
    # Test 5: Mixed Operations
    result = await run_benchmark(
        test_mixed_operations,
        num_requests=1000,
        concurrent=50
    )
    results.append(result)
    
    # Test 6: Failure Recovery
    result = await run_benchmark(test_failure_recovery)
    results.append(result)
    
    # Print summary
    print("\n" + "="*60)
    print("BENCHMARK RESULTS SUMMARY")
    print("="*60)
    
    for r in results:
        print(f"\n{r.name}:")
        print(f"  Throughput: {r.throughput:.2f} req/s")
        print(f"  Avg Latency: {r.avg_latency*1000:.2f} ms")
        print(f"  P95 Latency: {r.p95_latency*1000:.2f} ms")
        print(f"  P99 Latency: {r.p99_latency*1000:.2f} ms")
        print(f"  Success Rate: {r.successful_requests}/{r.total_requests} "
              f"({100*r.successful_requests/max(r.total_requests,1):.1f}%)")
    
    # Save results to JSON
    output = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "base_url": BASE_URL,
        "num_nodes": NUM_NODES,
        "results": [r.to_dict() for r in results]
    }
    
    with open("benchmark_results.json", "w") as f:
        json.dump(output, f, indent=2)
    
    print(f"\nResults saved to benchmark_results.json")
    
    return results



# MAIN


if __name__ == "__main__":
    print("Starting benchmarks...")
    print("Make sure all nodes are running before starting!")
    print("Run: docker-compose up -d")
    
    asyncio.run(run_all_benchmarks())