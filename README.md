# AadiMQ — Distributed Job Queue System

### Building a reliable background-processing engine with Node.js, Redis, and PostgreSQL

[![AadiMQ CI](https://github.com/azinel/AadiMQ/actions/workflows/ci.yml/badge.svg)](https://github.com/azinel/AadiMQ/actions/workflows/ci.yml)
[![Node.js](https://img.shields.io/badge/Node.js-18%20%7C%2020%20%7C%2022-brightgreen)](https://nodejs.org/)
[![License: ISC](https://img.shields.io/badge/License-ISC-blue.svg)](https://opensource.org/licenses/ISC)

AadiMQ is a distributed background job-processing system built from scratch with **Node.js ES Modules, Redis, and PostgreSQL**. It separates job submission from execution and coordinates independent workers through Redis-backed queues, atomic Lua operations, and renewable execution leases.

The project focuses on the engineering challenges behind asynchronous processing: concurrent job claims, worker coordination, stalled-job recovery, retries, delayed execution, priority scheduling, dead-letter handling, and operational visibility.

> AadiMQ is a production-oriented systems project. Reliability in a deployment still depends on the runtime topology, persistence configuration, monitoring, and application-level idempotency.

---

## Architecture

```mermaid
flowchart TD
    Client["Client / API Consumer"] -->|"POST /api/jobs"| API["Express API"]
    API --> Producer["Producer"]
    Producer -->|"Persist job record"| PG[("PostgreSQL")]
    Producer -->|"Queue metadata and state"| Redis[("Redis")]

    subgraph Runtime["Independent worker processes"]
      W1["Worker process 1"]
      W2["Worker process 2"]
      WN["Worker process N"]
    end

    Redis <-->|"Atomic Lua claim / lease operations"| Runtime
    Runtime -->|"Execution records and status"| PG

    subgraph Control["Queue control components"]
      Scheduler["Delayed-job Scheduler"]
      Recovery["Stalled-job Recovery Manager"]
      Heartbeat["Worker Heartbeat Manager"]
    end

    Scheduler --> Redis
    Recovery --> Redis
    Recovery --> PG
    Heartbeat --> Redis
    Heartbeat --> PG

    API --> Dashboard["Monitoring Dashboard"]
```

### Component responsibilities

| Component | Responsibility |
|---|---|
| **Producer** | Validates job input, checks idempotency keys, persists job records, and places jobs into FIFO, priority, or delayed Redis structures. |
| **Redis** | Coordinates waiting, priority, delayed, active, and dead-letter queues, along with worker and lease metadata. |
| **Worker** | Claims jobs, executes a supplied processor, renews leases, enforces execution timeouts, and records outcomes. |
| **Scheduler** | Promotes delayed jobs when their scheduled time arrives. |
| **Recovery Manager** | Detects expired active leases and requeues stalled jobs or moves exhausted jobs to the DLQ. |
| **PostgreSQL** | Stores durable job records, execution history, worker records, and queue metadata. |
| **Express API & Dashboard** | Exposes job and queue operations, health checks, worker information, and a browser-based monitoring view. |

---

## Core engineering concepts

### 1. Atomic job claiming

Workers compete for jobs from shared Redis data structures. Critical claim and state-transition operations are implemented as Lua scripts, so Redis-side changes execute atomically rather than as multiple interleavable client commands. The priority-aware claim path checks the priority sorted set before the FIFO waiting list, then moves the selected job into the active set and associates it with a worker lease.

### 2. Worker leases and heartbeats

A worker receives a lease when it claims a job. The heartbeat manager periodically renews active leases while processing continues. This helps distinguish healthy long-running work from jobs whose workers have stopped renewing their leases.

### 3. Fencing tokens

Claims use unique lock tokens. Completion and failure operations validate the token associated with the active claim before accepting a state transition. This prevents an outdated worker claim from acknowledging a job after it has been reclaimed by another worker.

### 4. Stalled-job recovery

The Recovery Manager scans active jobs for expired leases. It requeues recoverable jobs and moves jobs that have exhausted their allowed attempts to the dead-letter queue. Worker records can also be marked offline after heartbeat timeouts.

### 5. Retry and dead-letter handling

Failed jobs can be retried with exponential backoff. Once the configured attempt limit is exhausted, the job is moved to the **Dead Letter Queue (DLQ)**. A retry API allows a dead-lettered job to be replayed.

### 6. Scheduling and priority

- FIFO job processing.
- Priority-aware job selection.
- Delayed job submission and promotion.
- Job cancellation while pending.
- Sliding-window rate limiting through a Redis Lua script.

### 7. Idempotent submission

Job submission accepts an optional `idempotencyKey`. A unique constraint in PostgreSQL prevents the same key from creating multiple job records, supporting safe client retries after uncertain network responses.

---

## Redis Lua script registry

The queue's coordination operations are implemented as dedicated Lua scripts. Scripts are loaded into Redis and invoked with `EVALSHA`, with an `EVAL` fallback when necessary.

| Script | Purpose |
|---|---|
| `claim_job.lua` | Atomically claims a waiting job. |
| `claim_job_priority.lua` | Claims a priority job before falling back to FIFO. |
| `complete_job.lua` | Validates the lock token and marks a job complete. |
| `fail_job.lua` | Handles a failed job transition. |
| `fail_job_with_backoff.lua` | Schedules retry backoff or moves exhausted jobs to the DLQ. |
| `renew_lease.lua` | Renews an active job's lease. |
| `recover_stalled_jobs.lua` | Reclaims jobs with expired leases. |
| `promote_delayed_jobs.lua` | Moves matured delayed jobs into waiting queues. |
| `cancel_job.lua` | Removes a pending job and marks it cancelled. |
| `rate_limit.lua` | Applies a sliding-window rate limit. |
| `replay_dlq_job.lua` | Replays a dead-lettered job. |
| `enqueue_job.lua` | Atomic Redis enqueue operation available in the script registry. |

> **Implementation note:** The current `Producer.enqueue()` persists the PostgreSQL record and then writes Redis job metadata and the queue entry using Redis client commands. The presence of `enqueue_job.lua` in the registry should not be read as proof that the Producer currently invokes it.

---

## Technology stack

| Area | Technologies |
|---|---|
| Runtime | Node.js, JavaScript ES Modules |
| API | Express.js |
| Queue coordination | Redis 7, ioredis, Redis Lua |
| Persistent storage | PostgreSQL 16 |
| Data access | Prisma ORM |
| Input validation | Zod |
| Testing | Jest, Supertest, Redis Memory Server |
| Load testing | k6 scripts |
| Local infrastructure | Docker, Docker Compose |
| CI | GitHub Actions |

The CI workflow runs the test suite against Node.js **18.x, 20.x, and 22.x**, with PostgreSQL 16 and Redis 7 services.

---

## REST API

### Job management

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/jobs` | Submit a job with queue, priority, delay, retry limit, and optional idempotency key. |
| `GET` | `/api/jobs` | List jobs with pagination and queue/status filters. |
| `GET` | `/api/jobs/:id` | Retrieve job details. |
| `DELETE` | `/api/jobs/:id` | Cancel a pending or delayed job. |
| `POST` | `/api/jobs/:id/retry` | Replay a dead-lettered job. |

### Queues, workers, and health

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/queues` | List queues and their state counts. |
| `GET` | `/api/queues/:name/stats` | Retrieve queue execution and latency statistics. |
| `GET` | `/api/workers` | List registered workers and heartbeat information. |
| `GET` | `/api/health` | Check service dependencies. |
| `GET` | `/dashboard` | Open the monitoring dashboard. |

---

## Getting started

### Prerequisites

- Node.js 18 or later
- Docker and Docker Compose
- npm

### 1. Clone the repository

```bash
git clone https://github.com/azinel/AadiMQ.git
cd AadiMQ
npm install
```

### 2. Configure environment variables

Copy the example file:

```bash
cp .env.example .env
```

Configure the connection strings for your local PostgreSQL and Redis instances. A typical local configuration is:

```ini
PORT=3000
NODE_ENV=development
DATABASE_URL="postgresql://postgres:postgres@localhost:5432/aadimq?schema=public"
REDIS_URL="redis://127.0.0.1:6379"
DEFAULT_QUEUE="default"
WORKER_CONCURRENCY=2
JOB_LEASE_MS=30000
```

Use the variable names and any additional required settings from the repository's `.env.example` as the source of truth.

### 3. Start PostgreSQL and Redis

```bash
docker compose up -d postgres redis
```

### 4. Generate Prisma Client and sync the schema

```bash
npm run prisma:generate
npm run prisma:push
```

### 5. Start the API and worker

Run each process in a separate terminal:

```bash
# Terminal 1 — API and monitoring dashboard
npm start
```

```bash
# Terminal 2 — background worker
npm run worker
```

Open the dashboard at [http://localhost:3000/dashboard](http://localhost:3000/dashboard).

> The worker entry point currently uses a sample processor to simulate job execution. Replace it with your application-specific processor when integrating AadiMQ into another service.

---

## Testing

Run the test suite:

```bash
npm test
```

The repository's test suites cover:

| Test suite | Coverage |
|---|---|
| `tests/unit/job.test.js` | Job entity validation, serialization, and state behavior. |
| `tests/integration/phase1_queue.test.js` | Core enqueue, claim, execute, and completion flow. |
| `tests/integration/phase2_concurrency.test.js` | Concurrent processing and duplicate-claim checks across workers. |
| `tests/integration/phase3_coordination.test.js` | Worker registration, heartbeat lease renewal, and stalled-job recovery. |
| `tests/integration/phase4_fault_tolerance.test.js` | Retry backoff, DLQ transitions, timeout handling, and DLQ replay. |
| `tests/integration/phase5_scheduling.test.js` | Priority, delayed scheduling, cancellation, and rate limiting. |
| `tests/integration/phase6_monitoring.test.js` | Queue statistics and monitoring API behavior. |

The concurrency integration tests include a scenario with **50 jobs and 4 competing workers**, asserting that each job is completed once during that test run.

---

## Load testing

The `k6/` directory contains load-test scripts:

```bash
# Concurrent job submission
k6 run k6/enqueue_stress.js

# Read/query throughput
k6 run k6/throughput_benchmark.js
```

Run these against an isolated test environment, not a production database or queue.

---

## Run the full stack with Docker Compose

Build and start the API, worker, PostgreSQL, and Redis services:

```bash
docker compose up -d --build
```

| Service | Container | Role |
|---|---|---|
| PostgreSQL | `aadimq-postgres` | Persistent job and execution records. |
| Redis | `aadimq-redis` | Queue state, coordination, and leases; AOF persistence enabled. |
| API | `aadimq-api` | Express API and monitoring dashboard on port 3000. |
| Worker | `aadimq-worker` | Independent job-processing process. |

Stop the stack:

```bash
docker compose down
```

To also remove the local database and Redis data volumes:

```bash
docker compose down -v
```

> **Warning:** `docker compose down -v` permanently removes the named volumes created for this local stack.

---

## Repository structure

```text
AadiMQ/
├── .github/workflows/ci.yml
├── prisma/schema.prisma
├── public/
├── src/
│   ├── api/
│   │   ├── controllers/
│   │   ├── middlewares/
│   │   ├── routes/
│   │   └── server.js
│   ├── config/
│   ├── queue/
│   │   ├── job.js
│   │   ├── producer.js
│   │   └── stats.js
│   ├── redis/
│   │   ├── scripts/
│   │   ├── client.js
│   │   └── scriptLoader.js
│   ├── scheduler/
│   │   ├── recoveryManager.js
│   │   └── scheduler.js
│   ├── utils/
│   ├── worker/
│   │   ├── heartbeat.js
│   │   ├── worker.js
│   │   ├── workerPool.js
│   │   └── start.js
│   └── index.js
├── tests/
│   ├── unit/
│   └── integration/
├── k6/
├── docker-compose.yml
├── Dockerfile
└── package.json
```

---

## Engineering focus

AadiMQ is primarily a systems-engineering project. Its central challenges are coordinating concurrent consumers, defining safe job-state transitions, detecting stalled work, handling retries, and keeping operational state observable across Redis and PostgreSQL.

The project explores trade-offs in **at-least-once processing**, **idempotent submission**, **lease-based coordination**, and **recovery from worker failure**. These mechanisms do not make arbitrary job handlers exactly-once: application processors should still be idempotent, especially when performing external side effects.

---

## License

This repository currently declares the **ISC License** in `package.json`.