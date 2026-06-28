# Distributed Job Queue

A distributed job queue system written in Go, built incrementally to explore
the patterns behind reliable background processing — idempotent submission,
durable storage, retries, and (in later stages) message-broker fan-out to a
pool of workers.

This repository is being developed **feature by feature**. The sections marked
🚧 are scaffolded but not yet implemented.

---

## Why this exists

Background job processing is a solved problem in most languages —
Celery in Python, BullMQ in Node.js, Sidekiq in Ruby. Go has no
dominant equivalent. This project is a from-scratch implementation
of a production-grade job queue in Go, built to understand every
layer: durable submission, at-least-once delivery, concurrent worker
pools, exponential backoff, dead-letter queues, and distributed rate
limiting.

Typical use cases: file processing pipelines, email delivery,
payment webhooks, video transcoding — anything where an HTTP request
cannot block on slow work.

---

## Status

| Capability | State |
|---|---|
| Idempotent job submission over REST | ✅ Implemented |
| PostgreSQL-backed durable storage | ✅ Implemented |
| Job lookup by ID | ✅ Implemented |
| Health endpoint | ✅ Implemented |
| Kafka producer / consumer (KRaft, `franz-go`) | ✅ Implemented |
| Worker pool (goroutine-based execution) | ✅ Implemented |
| Retry, exponential backoff & dead-letter queue | ✅ Implemented |
| Redis sliding-window rate limiting | ✅ Implemented |
| Observability — metrics, structured logs | 🚧 Scaffolded |

---

## Design principles

These decisions are intentional and load-bearing — the system is built around them.

- **Client-generated IDs.** The client generates a UUID for each job *before*
  sending the request. The server never mints job IDs. This makes submission
  safely retryable.
- **Idempotency at the database.** `POST /jobs` checks PostgreSQL for the
  `job_id` before inserting. Re-sending the same job is a no-op that returns the
  existing record — so a client that times out and retries never creates a
  duplicate.
- **PostgreSQL as the source of truth.** Every job's full lifecycle state lives
  in one table. Kafka carries work *notifications*, not the authoritative state.
- **At-least-once delivery, made safe by idempotency.** The consumer commits a
  Kafka offset *only after* the job result is persisted to PostgreSQL. A crash
  between processing and commit means the job is redelivered — and the
  `completed` idempotency guard in the processor makes re-running it a no-op.
- **Rate limiting at the processor, not the edge.** The sliding-window check
  runs in `processor.go` right before work begins — so it throttles actual
  downstream load regardless of how jobs arrive (REST, Kafka redelivery, retry).
- **No frameworks, no ORM.** Routing uses [chi](https://github.com/go-chi/chi);
  database access uses [pgx/v5](https://github.com/jackc/pgx) with `pgxpool`;
  Kafka uses [franz-go](https://github.com/twmb/franz-go); Redis uses
  [go-redis/v9](https://github.com/redis/go-redis). No Gin/Fiber, no GORM.

---

## Architecture

```
                 ┌──────────────┐
   client ──────▶│   API server │  POST /jobs   (idempotent submit)
 (makes UUID)    │  (cmd/api)   │  GET  /jobs/:id
                 └──────┬───────┘
                        │ 1. insert (source of truth)
                        │ 2. publish to `jobs` topic
              ┌─────────┴─────────┐
              ▼                   ▼
       ┌────────────┐      ┌────────────┐
       │ PostgreSQL │      │   Kafka    │  topics: jobs, jobs.dlq
       │ (jobqueue) │      │  (KRaft)   │
       └─────▲──────┘      └─────┬──────┘
             │                   │ consume (group: job-workers)
             │ update status     ▼
             │            ┌──────────────┐     ┌────────────┐
             └────────────│ worker pool  │────▶│   Redis    │ rate_limit:{svc}
              commit after│ (cmd/worker) │     │ sliding win│ sorted set
              persist     └──────┬───────┘     └────────────┘
                                 │ retries exhausted → publish to jobs.dlq
                                 ▼
                          ┌────────────┐
                          │  jobs.dlq  │
                          └────────────┘
```

### Project layout

```
.
├── cmd/
│   ├── api/         # HTTP server entrypoint (publishes to Kafka on submit)
│   └── worker/      # worker entrypoint: consumer → pool → processor
├── internal/
│   ├── api/         # handlers + chi router
│   ├── store/       # pgxpool connection, migration, queries
│   ├── models/      # Job domain type
│   ├── config/      # env-var configuration
│   ├── worker/      # goroutine pool + processor (lifecycle, retries, DLQ)
│   ├── queue/       # Kafka producer + consumer (franz-go)
│   └── ratelimit/   # Redis sliding-window limiter
├── docker-compose.yml
├── Dockerfile
└── go.mod
```

---

## The `jobs` table

The migration runs automatically on API startup (`CREATE TABLE IF NOT EXISTS`).

| Column | Type | Notes |
|---|---|---|
| `job_id` | `TEXT` PK | client-supplied UUID |
| `payload` | `JSONB` | arbitrary job data |
| `status` | `TEXT` | `pending` / `running` / `completed` / `failed` |
| `attempt_count` | `INTEGER` | starts at 0 |
| `max_retries` | `INTEGER` | defaults to 3 |
| `last_error` | `TEXT` | nullable |
| `worker_id` | `TEXT` | nullable, set when claimed |
| `created_at` | `TIMESTAMPTZ` | |
| `updated_at` | `TIMESTAMPTZ` | |
| `started_at` | `TIMESTAMPTZ` | nullable |
| `completed_at` | `TIMESTAMPTZ` | nullable |

---

## API

### `GET /health`
Liveness check. → `200 OK` `{"status":"ok"}`

### `POST /jobs`
Submit a job. Body requires `job_id` (string) and `payload` (any valid JSON).
On a new job the API inserts it into PostgreSQL **and** publishes it to the
Kafka `jobs` topic (keyed by `job_id` so a job always maps to one partition).

| Response | Meaning |
|---|---|
| `202 Accepted` | new job created and enqueued to Kafka |
| `200 OK` | job already existed — idempotency hit, returns the stored job |
| `400 Bad Request` | missing `job_id` / `payload` or invalid JSON |
| `500` | database error, or Kafka publish failed (the PG insert is *not* rolled back — the job is recorded as `pending` and can be re-published) |

### `GET /jobs/{id}`
Fetch a job by ID. → `200 OK` with the job, or `404 Not Found`.

---

## Getting started

### Prerequisites
- Go 1.22+
- Docker + Docker Compose

### 1. Start the infrastructure
```bash
docker-compose up -d
```
Brings up, each with a health check:
- **PostgreSQL 16** — db `jobqueue`, user/password `postgres`, port `5432`
- **Kafka** (`confluentinc/cp-kafka`, KRaft mode — no Zookeeper), port `9092`.
  Topics `jobs` and `jobs.dlq` are auto-created on first use.
- **Redis 7** — port `6379`

### 2. Run the API server
```bash
go mod tidy
go run ./cmd/api
```
Listens on `:8080` by default.

### 3. Run the worker
```bash
go run ./cmd/worker
```
Joins the `job-workers` consumer group, starts a pool of goroutines, and begins
draining the `jobs` topic. The API and worker are separate binaries and can be
run/scaled independently.

### Configuration

All config comes from environment variables (with defaults):

| Variable | Default | Used by |
|---|---|---|
| `DB_HOST` | `localhost` | api, worker |
| `DB_PORT` | `5432` | api, worker |
| `DB_USER` | `postgres` | api, worker |
| `DB_PASSWORD` | `postgres` | api, worker |
| `DB_NAME` | `jobqueue` | api, worker |
| `SERVER_PORT` | `8080` | api |
| `KAFKA_BROKERS` | `localhost:9092` | api, worker |
| `KAFKA_TOPIC` | `jobs` | api, worker |
| `KAFKA_DLQ_TOPIC` | `jobs.dlq` | worker |
| `KAFKA_GROUP_ID` | `job-workers` | worker |
| `WORKER_POOL_SIZE` | `10` | worker |
| `REDIS_ADDR` | `localhost:6379` | worker |
| `REDIS_PASSWORD` | `` (empty) | worker |
| `REDIS_DB` | `0` | worker |
| `RATE_LIMIT_WINDOW_SECONDS` | `60` | worker |
| `RATE_LIMIT_MAX_REQUESTS` | `100` | worker |
| `RATE_LIMIT_RETRY_MS` | `100` | worker |

---

## Trying it out

Create a job (note: the client picks the UUID):
```bash
curl -i -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "job_id": "550e8400-e29b-41d4-a716-446655440000",
    "payload": {"task": "send_email", "to": "user@example.com"}
  }'
# → 202 Accepted   (first time)
# → 200 OK         (same job_id again — idempotency hit)
```

Fetch it back — with the worker running, you'll watch it move
`pending → running → completed`:
```bash
curl -i http://localhost:8080/jobs/550e8400-e29b-41d4-a716-446655440000
# → 200 OK
# {"job_id":"550e8400-...","payload":{"task":"send_email","to":"user@example.com"},
#  "status":"completed","attempt_count":0,"max_retries":3,"worker_id":"...", ...}
```

---

## How a job is processed

Each worker goroutine pulls a job off the in-memory channel (fed by the Kafka
consumer) and runs it through `processor.go`:

1. **Idempotency guard** — if the job is already `completed` in PostgreSQL, skip
   it and return (safe under at-least-once redelivery).
2. **Rate limit** — `limiter.Allow(ctx, "job_processor")` blocks until the
   sliding window admits the request (see below).
3. **`running`** — set status + `worker_id` + `started_at`.
4. **Work** — (simulated with a short sleep).
5. **`completed`** — set status + `completed_at`.

The Kafka offset is committed **only after** the result is persisted. Every
goroutine has a `defer recover()` so a single bad job can never crash the pool.

### Retries, backoff, and the dead-letter queue

On failure the processor increments `attempt_count` and stores `last_error`:

- `attempt_count < max_retries` → republish to `jobs` with exponential backoff
  (`2s · 2^attempt + random(0–1s)`).
- `attempt_count >= max_retries` → mark the job `failed` and publish the
  enriched job record to the **`jobs.dlq`** topic.

Inspect the dead-letter queue:
```bash
docker exec jobqueue-kafka \
  kafka-console-consumer --topic jobs.dlq --from-beginning \
  --bootstrap-server localhost:9092
```

### Rate limiting (Redis sliding window)

Throttling runs in the processor, gating real downstream work. Each service has
a Redis sorted set `rate_limit:{service}`; a single Lua script atomically evicts
entries older than the window, counts the rest, and admits (recording a UUID
member) only if under the limit. When throttled the worker sleeps
`RATE_LIMIT_RETRY_MS` and retries — it never drops the job.

```bash
# watch the window fill up while jobs are processing
docker exec jobqueue-redis redis-cli ZCARD rate_limit:job_processor
docker exec jobqueue-redis redis-cli ZRANGE rate_limit:job_processor 0 -1 WITHSCORES

# demo: cap at 2 concurrent, submit 5 jobs — 2 proceed, 3 queue behind the limiter
RATE_LIMIT_MAX_REQUESTS=2 go run ./cmd/worker
```

---

## Roadmap

1. ✅ **Idempotent submission + durable storage**
2. ✅ **Kafka integration** — producer on submit, consumer feeding the pool
3. ✅ **Worker pool** — goroutine-based execution with panic recovery
4. ✅ **Retries & failure handling** — `attempt_count`, `max_retries`, backoff, DLQ
5. ✅ **Rate limiting** — Redis sliding-window throttle at the processor
6. 🚧 **Observability** — metrics, structured logs, tracing
