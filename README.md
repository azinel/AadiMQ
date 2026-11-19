# AadiMQ - Distributed Job Queue System
### Production-Grade Backend & Distributed Systems Architecture

[![AadiMQ CI Pipeline](https://github.com/azinel/AadiMQ/actions/workflows/ci.yml/badge.svg)](https://github.com/azinel/AadiMQ/actions)
[![Node.js](https://img.shields.io/badge/Node.js-22.x%20%7C%2020.x%20%7C%2018.x-brightgreen.svg)](https://nodejs.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**AadiMQ** is an industrial-strength, distributed background job processing engine built completely from scratch using **plain JavaScript (Node.js ES Modules)**, **Redis**, and **PostgreSQL**.

Architecturally designed with the operational resilience of **BullMQ**, **Celery**, and **AWS SQS**, AadiMQ enforces **atomic state transitions via custom Redis Lua scripts**, **cryptographic fencing tokens**, **worker lease heartbeats**, **crash recovery daemons**, **exponential backoff**, **Dead Letter Queues**, and **real-time observability**.

---

## 1. High-Level System Architecture

```mermaid
flowchart TD
    Client["Client / API Consumer"] -->|HTTP POST /api/jobs| API["API Server (Express)"]
    API -->|1. Idempotency Check & Validate| Producer["Producer Engine"]
    Producer -->|2. Write System-of-Record| PG[("PostgreSQL\n(Jobs & Executions)")]
    Producer -->|3. Atomic Enqueue| Redis[("Redis\n(Coordination & Leases)")]

    subgraph Workers["Distributed Worker Fleet"]
        W1["Worker 1 (C=4)"]
        W2["Worker 2 (C=4)"]
        WN["Worker N (C=4)"]
    end

    Redis <-->|Atomic Lua Claims & Heartbeats| Workers
    Workers -->|Status Audit & Results| PG

    subgraph Daemons["Control Plane Daemons"]
        Scheduler["Scheduler (Delayed Promotion)"]
        Recovery["Recovery Manager (Crash Detection)"]
    end

    Scheduler <-->|ZRANGEBYSCORE promote| Redis
    Recovery <-->|Scan Expired Leases| Redis
    Recovery -->|Sync Reclaimed Jobs| PG
    Scheduler -->|Sync Promoted Jobs| PG

    Dashboard["Monitoring Web Dashboard"] <-->|Real-time Poll| API
```

---

## 2. Distributed Systems Invariants & Design Principles

### A. At-Least-Once Delivery & Idempotency
- **No Lost Jobs**: Every job written to PostgreSQL is backed by Redis queues. If a worker crashes midway, the job lease expires and the **Recovery Manager** automatically returns it to the active pool.
- **Deduplication**: Job submissions accept an optional `idempotencyKey` backed by a unique PostgreSQL constraint, preventing duplicate executions caused by client network timeouts or retries.

### B. Split-Brain & Zombie Worker Prevention (Fencing Tokens)
When Worker $A$ claims a job, it is issued a unique cryptographic `lockToken`. If Worker $A$ pauses (e.g. GC pause or network partition), its lease in Redis expires. The Recovery Manager reassigns the job to Worker $B$ with a new `lockToken`. When Worker $A$ awakens and attempts to acknowledge completion via `complete_job.lua`, Redis detects the token mismatch (`ERR_FENCE_MISMATCH`) and **rejects Worker $A$'s acknowledgment**, preventing database corruption.

### C. Atomic Transitions via Precompiled Lua Scripts
Standard multi-key Redis commands without Lua introduce race conditions under high concurrency. All state transitions in AadiMQ are atomic Lua scripts loaded by SHA1 digest (`EVALSHA`):

| Script | Redis Primitives | Purpose |
| :--- | :--- | :--- |
| `enqueue_job.lua` | `HSET`, `LPUSH` | Atomically creates `job:<id>` Hash and pushes to `queue:<name>:waiting`. |
| `claim_job_priority.lua` | `ZPOPMAX`, `RPOP`, `ZADD`, `HSET` | Atomically prioritizes `waiting:priority` ZSET over `waiting` FIFO list, moves to `active` ZSET with lease score, and issues `lockToken`. |
| `complete_job.lua` | `HGET`, `ZREM`, `HSET`, `EXPIRE` | Verifies fencing lock token, removes from `active`, marks `COMPLETED`, sets retention TTL. |
| `fail_job_with_backoff.lua` | `ZREM`, `ZADD`, `LPUSH`, `HSET` | Verifies lock token, computes exponential backoff score into `delayed` ZSET, or routes to `dlq` list if `attempts >= maxAttempts`. |
| `renew_lease.lua` | `HGET`, `ZADD` | Renews the lease expiration timestamp in `active` ZSET for in-flight tasks. |
| `recover_stalled_jobs.lua` | `ZRANGEBYSCORE`, `ZREM`, `LPUSH` | Scans `active` ZSET where score $\le now$, reclaiming abandoned jobs to `waiting` or `dlq`. |
| `promote_delayed_jobs.lua` | `ZRANGEBYSCORE`, `ZREM`, `ZADD`, `LPUSH` | Matures delayed jobs whose timestamp has arrived into active waiting queues. |
| `cancel_job.lua` | `LREM`, `ZREM`, `HSET` | Atomically removes jobs from pending queues and marks `CANCELLED`. |
| `rate_limit.lua` | `ZREMRANGEBYSCORE`, `ZCARD`, `ZADD` | Sliding-window rate limiter preventing queue exhaustion. |

---

## 3. Technology Stack

- **Runtime**: Node.js v18+ (Plain JavaScript ES Modules: `"type": "module"`)
- **Web API**: Express.js
- **Coordination Engine**: Redis 7 (via `ioredis` with exponential connection retries)
- **Persistent Database**: PostgreSQL 16
- **ORM / Migrations**: Prisma ORM
- **Testing**: Jest (Experimental VM Modules), Supertest, `redis-memory-server`
- **Load Testing**: k6
- **Containerization**: Docker & Docker Compose
- **CI/CD**: GitHub Actions

---

## 4. REST API Specification

### Job Management
| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `POST` | `/api/jobs` | Enqueue a job (supports priority, delay, maxAttempts, idempotencyKey). |
| `GET` | `/api/jobs/:id` | Fetch job details (checks Redis fast path, falls back to PostgreSQL). |
| `GET` | `/api/jobs` | List jobs with pagination (`page`, `limit`) and filtering (`queue`, `status`). |
| `DELETE` | `/api/jobs/:id` | Cancel a pending or delayed job before execution. |
| `POST` | `/api/jobs/:id/retry` | Replay / retry a dead-lettered job from DLQ. |

### Queues & Metrics
| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `GET` | `/api/queues` | Returns all queues with counts (waiting, active, delayed, DLQ, completed). |
| `GET` | `/api/queues/:name/stats` | Deep latency metrics (avg, min, max execution duration) for a queue. |
| `GET` | `/api/workers` | Real-time worker registry, active job counts, and heartbeat statuses. |
| `GET` | `/api/health` | Health probe checking Redis and PostgreSQL connectivity. |
| `GET` | `/dashboard` | Live HTML5 / React monitoring dashboard. |

---

## 5. Getting Started & Local Development

### 1. Clone & Install Dependencies
```bash
git clone https://github.com/azinel/AadiMQ.git
cd AadiMQ
npm install
```

### 2. Environment Configuration
Create a `.env` file from the template:
```bash
cp .env.example .env
```

Ensure `DATABASE_URL` and `REDIS_URL` point to your local or containerized instances:
```ini
PORT=3000
NODE_ENV=development
DATABASE_URL="postgresql://postgres:postgres@localhost:5432/aadimq?schema=public"
REDIS_URL="redis://127.0.0.1:6379"
DEFAULT_QUEUE="default"
WORKER_CONCURRENCY=2
JOB_LEASE_MS=30000
```

### 3. Start Database and Redis via Docker Compose
```bash
docker compose up -d postgres redis
```

### 4. Push Prisma Database Schema
```bash
npm run prisma:generate
npm run prisma:push
```

### 5. Start API Server & Workers
```bash
# Terminal 1: Start API Server & Monitoring Dashboard
npm start

# Terminal 2: Start Background Worker Process
npm run worker
```

Access the **Live Monitoring Dashboard** at [http://localhost:3000/dashboard](http://localhost:3000/dashboard).

---

## 6. Automated Testing

AadiMQ includes comprehensive unit and integration test suites covering all phases:
- `tests/unit/job.test.js`: Domain entity serialization, validation, and terminal states.
- `tests/integration/phase1_queue.test.js`: Core FIFO claim/execute/complete lifecycle.
- `tests/integration/phase2_concurrency.test.js`: Multi-worker race-condition tests (50 jobs across 4 competing workers with 0 duplicate claims).
- `tests/integration/phase3_coordination.test.js`: Heartbeat lease renewal & worker crash recovery.
- `tests/integration/phase4_fault_tolerance.test.js`: Exponential backoff, timeout protection, DLQ replay.
- `tests/integration/phase5_scheduling.test.js`: Priority ordering, delayed job scheduler promotion, job cancellation, sliding-window rate limiting.
- `tests/integration/phase6_monitoring.test.js`: Statistics aggregation and dashboard endpoints.

Run the entire automated test suite:
```bash
npm test
```

---

## 7. Performance Benchmarking with k6

Load test scripts are located in `k6/`:

```bash
# 1. Enqueue stress test (50 Virtual Users submitting jobs concurrently)
k6 run k6/enqueue_stress.js

# 2. Read query throughput benchmark (health, queue stats, worker fleet)
k6 run k6/throughput_benchmark.js
```

---

## 8. Deployment via Docker Compose

To deploy the entire distributed system (API Server + Background Worker Pool + PostgreSQL + Redis):
```bash
docker compose up -d --build
```
This boots:
- `aadimq-postgres`: PostgreSQL 16 container with durable volume.
- `aadimq-redis`: Redis 7 Alpine container with AOF persistence.
- `aadimq-api`: Express API Server with health checks on port 3000.
- `aadimq-worker`: Independent Worker process automatically processing jobs.
#   a a d i M Q  
 