# Dokumentasi Arsitektur Sistem

## Daftar Isi
1. [Gambaran Umum](#gambaran-umum)
2. [Penjelasan Setiap File](#penjelasan-setiap-file)
3. [Alur Kerja Sistem](#alur-kerja-sistem)
4. [Algoritma yang Digunakan](#algoritma-yang-digunakan)

---

## Gambaran Umum

Sistem ini terdiri dari 3 node identik yang berjalan secara paralel. Setiap node menjalankan empat komponen utama secara bersamaan: Raft Consensus, Distributed Lock Manager, Distributed Queue, dan Cache Coherence. Semua node berkomunikasi melalui HTTP, dan menggunakan Redis sebagai penyimpanan state bersama untuk persistensi data.

```
Client
  │
  ▼
Node 1 (Leader)  ◄──── Raft Heartbeat ────► Node 2 (Follower)
  │                                              │
  │              ◄──── Raft Heartbeat ────► Node 3 (Follower)
  │
  ▼
Redis (Shared State)
```

Hanya Leader yang menerima write request dari client. Follower meneruskan request ke Leader jika ada client yang salah kirim. Semua write melewati Raft log sebelum dieksekusi, sehingga seluruh node selalu memiliki state yang sama.

---

## Penjelasan Setiap File

### `src/utils/config.py`

File ini adalah pusat konfigurasi seluruh sistem. Semua nilai konfigurasi dibaca dari environment variables (file `.env`) menggunakan library `python-dotenv`. Terdapat 6 dataclass di dalamnya:

- `NodeConfig` — menyimpan identitas node: `node_id`, `host`, dan `port`.
- `ClusterConfig` — menyimpan daftar semua node dalam cluster dan menghitung `quorum` (jumlah minimum node untuk mayoritas).
- `RedisConfig` — konfigurasi koneksi Redis termasuk URL lengkap dengan password opsional.
- `RaftConfig` — parameter algoritma Raft: election timeout, heartbeat interval, dan RPC timeout.
- `LockConfig` — konfigurasi Lock Manager: TTL default, TTL maksimum, retry interval, dan max retries.
- `CacheConfig` — konfigurasi Cache: protokol MESI/MOSI/MOESI, ukuran maksimum, dan replacement policy.

Semua modul lain mengimport singleton `config` dari file ini: `from src.utils.config import config`.

---

### `src/utils/metrics.py`

File ini mendefinisikan semua metrik Prometheus yang dikumpulkan oleh sistem. Menggunakan library `prometheus-client` untuk mengekspos metrik melalui HTTP endpoint yang di-scrape oleh Prometheus.

Metrik yang dikumpulkan dibagi per komponen:

- **Raft**: `raft_node_role` (0=follower, 1=candidate, 2=leader), `raft_current_term`, jumlah election, dan jumlah heartbeat yang dikirim.
- **Lock Manager**: total request lock beserta hasilnya (granted/denied/timeout), jumlah lock aktif, latency operasi acquire/release, dan jumlah deadlock terdeteksi.
- **Queue**: jumlah pesan yang diproduksi dan dikonsumsi per queue, serta kedalaman antrian.
- **Cache**: jumlah cache hit dan miss, jumlah invalidasi, dan ukuran cache saat ini.

Singleton `metrics` diimport oleh semua komponen untuk mencatat data performa.

---

### `src/consensus/raft.py`

File terpenting dalam sistem. Mengimplementasikan algoritma Raft Consensus secara lengkap dengan tiga sub-masalah:

**Leader Election**: Setiap node memiliki election timer dengan nilai acak antara `RAFT_ELECTION_TIMEOUT_MIN` dan `RAFT_ELECTION_TIMEOUT_MAX` (default 150–300ms). Jika node tidak menerima heartbeat dalam waktu tersebut, ia menjadi Candidate dan memulai election dengan mengirim `RequestVote` ke semua peer. Node yang mendapat suara mayoritas (lebih dari setengah jumlah node) menjadi Leader.

**Log Replication**: Semua perintah dari client masuk melalui `submit_command()` yang hanya bisa dipanggil di Leader. Leader menyimpan perintah ke log-nya, lalu mengirim `AppendEntries` ke semua Follower. Entry dianggap "committed" ketika mayoritas node sudah menyimpannya.

**Safety**: Implementasi "election restriction" memastikan hanya candidate dengan log paling up-to-date yang bisa menang election. Ini mencegah data yang sudah committed hilang ketika terjadi failover.

Kelas dan dataclass utama dalam file ini:
- `NodeRole` — enum dengan tiga nilai: FOLLOWER, CANDIDATE, LEADER.
- `LogEntry` — satu baris dalam log: berisi term, index, command, dan status committed.
- `VoteRequest` / `VoteResponse` — struktur pesan untuk proses election.
- `AppendEntriesRequest` / `AppendEntriesResponse` — struktur pesan untuk replikasi log dan heartbeat.
- `RaftNode` — kelas utama yang menjalankan semua logika Raft.

Method penting di `RaftNode`:
- `start()` — memulai background tasks (election timer dan apply loop).
- `submit_command(command)` — menerima perintah dari Lock Manager/Queue/Cache, hanya berhasil di Leader.
- `wait_for_commit(index)` — menunggu sampai log entry pada index tertentu sudah committed.
- `handle_vote_request()` — memproses RequestVote yang masuk dari Candidate lain.
- `handle_append_entries()` — memproses AppendEntries yang masuk dari Leader.
- `register_state_machine(callback)` — mendaftarkan fungsi yang dipanggil setiap kali ada entry baru yang committed.

---

### `src/communication/message_passing.py`

File ini menangani komunikasi HTTP antar node. `RaftNode` tidak tahu bagaimana cara mengirim pesan ke jaringan — tugasnya hanya logika Raft. `MessagePassing` yang menjadi jembatan antara Raft dengan jaringan.

Cara kerjanya:
1. Saat startup, `MessagePassing` membangun peta `node_id → URL` dari konfigurasi cluster.
2. Ia mendaftarkan dirinya sebagai `rpc_sender` di `RaftNode`, sehingga Raft bisa memanggil `send_vote_request()` dan `send_append_entries()` tanpa tahu detailnya.
3. Untuk incoming RPC, ia menyediakan HTTP handler `handle_vote_request()` dan `handle_append_entries()` yang didaftarkan ke web server di route `/raft/vote` dan `/raft/append`.

Semua outgoing request menggunakan `aiohttp.ClientSession` dengan timeout sesuai `RAFT_RPC_TIMEOUT`. Jika peer tidak merespons (timeout atau connection error), ia dianggap tidak vote / gagal append dan proses dilanjutkan.

---

### `src/communication/failure_detector.py`

File ini mengimplementasikan failure detector sederhana yang terinspirasi dari Phi Accrual Failure Detector. Setiap peer di-ping melalui HTTP GET ke endpoint `/health` secara berkala (default setiap 1 detik).

Status setiap peer:
- `ALIVE` — merespons dengan baik.
- `SUSPECT` — gagal merespons 3 kali berturut-turut.
- `DEAD` — gagal merespons 6 kali berturut-turut.

Ketika status berubah menjadi DEAD, callback `on_node_dead` dipanggil. Ketika node yang mati kembali merespons, callback `on_node_recovered` dipanggil. Callback ini digunakan oleh `Node` di `base_node.py` untuk mengambil tindakan, misalnya melepas lock yang dipegang node yang mati.

---

### `src/nodes/lock_manager.py`

Mengimplementasikan Distributed Lock Manager di atas Raft Consensus. Semua operasi lock (acquire dan release) terlebih dahulu masuk ke Raft log sebelum dieksekusi, sehingga semua node memiliki state lock yang sama.

**Tipe Lock**:
- `SHARED` — beberapa client bisa memegang lock yang sama secara bersamaan, selama tidak ada yang exclusive.
- `EXCLUSIVE` — hanya satu client yang boleh memegang lock, tidak ada client lain yang bisa acquire.

**Alur Acquire Lock**:
1. Cek apakah Raft node ini adalah Leader. Jika bukan, tolak request.
2. Cek apakah lock bisa diperoleh (`_can_acquire()`).
3. Jika bisa, submit command `LOCK_ACQUIRE` ke Raft log.
4. Tunggu sampai command committed (mayoritas node menyimpannya).
5. Return `LockResult` dengan status `GRANTED`.
6. Jika tidak bisa karena ada lock lain, masukkan ke wait queue dan coba lagi setelah interval tertentu.
7. Sebelum retry, jalankan deadlock detection. Jika ada siklus dalam wait-for graph, batalkan dan return `DENIED`.

**Deadlock Detection**: Menggunakan algoritma DFS (Depth-First Search) pada "wait-for graph". Graph ini merepresentasikan relasi "client A menunggu client B" ketika A ingin lock yang sedang dipegang B. Jika ditemukan siklus, deadlock terdeteksi.

**Persistensi**: Setiap lock yang aktif disimpan ke Redis dengan TTL yang sesuai. Saat node restart, lock dipulihkan dari Redis melalui `_sync_from_redis()`.

**TTL Expiry**: Background loop berjalan setiap 1 detik untuk memeriksa lock yang sudah expired dan melepasnya otomatis.

Kelas utama:
- `LockInfo` — menyimpan info satu lock: resource, tipe, client, TTL, dan waktu expired.
- `LockRequest` / `LockResult` — struktur input dan output operasi lock.
- `WaitQueue` — antrian berbasis `asyncio.Queue` untuk client yang menunggu lock.
- `DeadlockDetector` — deteksi siklus dengan DFS pada wait-for graph.
- `LockManager` — kelas utama yang mengkoordinasikan semua komponen di atas.

---

### `src/nodes/queue_node.py`

Mengimplementasikan Distributed Queue menggunakan Consistent Hashing untuk menentukan node mana yang bertanggung jawab atas suatu queue.

**Consistent Hashing**: `ConsistentHashRing` menempatkan node-node pada "ring" virtual menggunakan hash MD5. Setiap queue name di-hash dan diletakkan di ring — node terdekat di ring (searah jarum jam) adalah primary node untuk queue tersebut. Dengan replication factor = 2, pesan juga disimpan di node berikutnya di ring. Ini memastikan pesan tidak hilang meskipun satu node mati.

**Alur Produce**:
1. Generate `message_id` unik (UUID).
2. Submit command `QUEUE_PRODUCE` ke Raft log.
3. Tunggu commit, lalu return `message_id` ke producer.
4. Di semua node, `_apply_command()` menyimpan pesan ke local queue dan ke Redis.

**Alur Consume**:
1. Ambil pesan pertama dari local queue (FIFO).
2. Pindahkan pesan ke `_in_flight` dictionary dengan deadline ACK.
3. Return pesan ke consumer.
4. Consumer harus memanggil `/queue/ack` dalam batas waktu `ack_timeout`.

**At-Least-Once Delivery**: Jika consumer tidak mengirim ACK sebelum `ack_deadline`, pesan dikembalikan ke depan antrian oleh `_redelivery_loop()` dan akan dikirim ulang ke consumer berikutnya. Ini menjamin pesan tidak hilang, tetapi consumer harus idempotent (siap menerima pesan duplikat).

**Persistensi**: Setiap pesan disimpan ke Redis. Saat node restart, semua pesan dipulihkan dari Redis melalui `_restore_from_redis()`.

Kelas utama:
- `Message` — satu pesan dalam queue.
- `ConsistentHashRing` — implementasi consistent hashing dengan virtual nodes.
- `QueueNode` — kelas utama yang mengkoordinasikan semua operasi queue.

---

### `src/nodes/cache_node.py`

Mengimplementasikan Distributed Cache Coherence dengan protokol MESI. Protokol MESI memastikan semua cache node memiliki pandangan yang konsisten terhadap data yang sama.

**State MESI**:
- `Modified (M)` — cache entry ini telah dimodifikasi dan berbeda dari nilai di Redis (dirty). Hanya satu cache yang boleh dalam state M untuk key yang sama.
- `Exclusive (E)` — cache entry sama dengan Redis, dan hanya cache ini yang menyimpannya (tidak ada cache lain).
- `Shared (S)` — beberapa cache menyimpan entry yang sama, semua konsisten dengan Redis.
- `Invalid (I)` — cache entry tidak valid, harus di-fetch ulang dari Redis.

**Alur Set**:
1. Submit command `CACHE_SET` ke Raft log.
2. Di semua node, `_apply_command()` menyimpan entry baru. Jika key sudah ada → state `MODIFIED`, jika baru → state `EXCLUSIVE`.
3. Entry juga disimpan ke Redis untuk persistensi.

**Alur Invalidation**:
1. Submit command `CACHE_INVALIDATE` ke Raft log.
2. Di semua node, entry di-mark sebagai `INVALID` di local cache dan dihapus dari Redis.
3. Notifikasi juga dikirim via Redis pub/sub channel `cache:invalidate` agar semua node yang tidak ikut Raft quorum juga menginvalidasi entry-nya.

**Replacement Policy**: Tersedia dua implementasi:
- `LRUCache` — menggunakan `OrderedDict` Python. Entry yang paling jarang diakses belakangan akan dibuang saat cache penuh.
- `LFUCache` — menggunakan dictionary biasa + frekuensi counter. Entry dengan frekuensi akses paling rendah akan dibuang.

Pilihan policy dikonfigurasi via `CACHE_REPLACEMENT_POLICY` di `.env`.

---

### `src/nodes/base_node.py`

Entry point yang dijalankan di setiap container. File ini tidak mengandung logika bisnis — tugasnya hanya mengkoordinasikan startup dan shutdown semua komponen, serta mendaftarkan HTTP routes.

Urutan startup di `Node.start()`:
1. Jalankan Prometheus metrics server.
2. Buka HTTP client session untuk outgoing RPC (`MessagePassing.start()`).
3. Mulai failure detector.
4. Mulai Raft consensus engine.
5. Mulai Lock Manager (termasuk koneksi Redis dan sync dari Redis).
6. Mulai Queue Node.
7. Mulai Cache Node.
8. Buka HTTP server untuk menerima request.

Urutan shutdown di `Node.stop()` kebalikannya — komponen yang paling bergantung dimatikan lebih dulu.

Helper `_leader_only()` memeriksa apakah node ini adalah Leader sebelum memproses write request. Jika bukan, ia mengembalikan HTTP 307 beserta URL Leader yang sebenarnya, sehingga client tahu ke mana harus redirect.

---

### `tests/unit/test_raft.py`

Unit test untuk memverifikasi kebenaran implementasi Raft. Menggunakan `pytest` dan `pytest-asyncio` untuk test async.

Test cases yang ada:
- `TestInitialState` — memverifikasi bahwa node baru selalu mulai sebagai Follower dengan term 0 dan log kosong.
- `TestVoteRequest` — memverifikasi aturan voting: kasih suara jika term lebih tinggi dan log up-to-date, tolak jika sudah vote di term yang sama, tolak jika log candidate lebih pendek.
- `TestAppendEntries` — memverifikasi bahwa heartbeat mereset election timer, AppendEntries dengan stale term ditolak, entry baru ditambahkan dengan benar, dan commit index diupdate.
- `TestSubmitCommand` — memverifikasi bahwa hanya leader yang bisa menerima command dan command masuk ke log dengan benar.

Cara menjalankan:
```bash
pytest tests/unit/test_raft.py -v
```

---

## Alur Kerja Sistem

### Skenario 1: Client Meminta Lock

```
Client → POST /lock/acquire → Node 1 (Leader)
  Node 1: cek _can_acquire() → True
  Node 1: submit_command(LOCK_ACQUIRE) → tambah ke log index=5
  Node 1: kirim AppendEntries ke Node 2 dan Node 3
  Node 2: simpan entry index=5, balas ACK
  Node 3: simpan entry index=5, balas ACK
  Node 1: mayoritas (3/3) → commit index=5
  Node 1: apply_command() → simpan lock ke _locks dan Redis
  Node 1 → Client: {"status": "granted"}
```

### Skenario 2: Node Leader Crash

```
Node 1 (Leader) crash tiba-tiba
Node 2: election timeout (misal 200ms) → mulai election
Node 2: kirim RequestVote ke Node 3
Node 3: kasih suara ke Node 2
Node 2: dapat 2/3 suara → jadi Leader baru
Node 2: kirim heartbeat ke Node 3
Node 1: kembali online → lihat term lebih tinggi → jadi Follower
Node 1: minta log yang tertinggal via AppendEntries
```

### Skenario 3: Cache Invalidation

```
Client → DELETE /cache/invalidate/user:123 → Node 2 (Leader)
  Node 2: submit_command(CACHE_INVALIDATE key=user:123)
  Node 2: entry direplikasi dan committed
  Semua node: apply_command() → mark entry INVALID di local cache
  Node 2: broadcast via Redis pub/sub "cache:invalidate"
  Node 3: terima notifikasi → invalidate local cache
  Selanjutnya GET /cache/get/user:123 → miss → fetch dari Redis
```

---

## Algoritma yang Digunakan

| Komponen | Algoritma | Referensi |
|---|---|---|
| Consensus | Raft | Ongaro & Ousterhout, 2014 |
| Lock Ordering | Timestamp-based (via Raft term+index) | Lamport, 1978 |
| Deadlock Detection | DFS pada Wait-For Graph | Tanenbaum, Distributed Systems |
| Queue Distribution | Consistent Hashing | Karger et al., 1997 |
| Cache Coherence | MESI Protocol | Papamarcos & Patel, 1984 |
| Cache Eviction | LRU (OrderedDict) / LFU (frequency counter) | — |
| Failure Detection | Simplified Phi Accrual | Hayashibara et al., 2004 |