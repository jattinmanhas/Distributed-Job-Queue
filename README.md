# Distributed Job Queue

A distributed job queue system written in Go, built incrementally to explore
the patterns behind reliable background processing — idempotent submission,
durable storage, retries, and (in later stages) message-broker fan-out to a
pool of workers.

This repository is being developed **feature by feature**. The sections marked
🚧 are scaffolded but not yet implemented.

---

## Status

| Capability | State |
|---|---|
| Idempotent job submission over REST | ✅ Implemented |
| PostgreSQL-backed durable storage | ✅ Implemented |
| Job lookup by ID | ✅ Implemented |
| Health endpoint | ✅ Implemented |
| Worker pool (goroutine-based execution) | 🚧 Scaffolded |
| Kafka producer / consumer | 🚧 Scaffolded |
| Retry & failure handling | 🚧 Scaffolded |

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
  in one table. The message broker (added later) carries work *notifications*,
  not the authoritative state.
- **No frameworks, no ORM.** Routing uses [chi](https://github.com/go-chi/chi);
  database access uses [pgx/v5](https://github.com/jackc/pgx) with `pgxpool`.
  No Gin/Fiber, no GORM.

---

## Architecture

```
                 ┌──────────────┐
   client ──────▶│   API server │  POST /jobs   (idempotent submit)
 (makes UUID)    │  (cmd/api)   │  GET  /jobs/:id
                 └──────┬───────┘
                        │
                        ▼
                 ┌──────────────┐
                 │  PostgreSQL  │  jobs table = source of truth
                 │   (jobqueue) │
                 └──────────────┘

   🚧 future: API ──▶ Kafka ──▶ worker pool (cmd/worker) ──▶ updates jobs table
```

### Project layout

```
.
├── cmd/
│   ├── api/         # HTTP server entrypoint
│   └── worker/      # worker entrypoint (🚧 scaffold)
├── internal/
│   ├── api/         # handlers + chi router
│   ├── store/       # pgxpool connection, migration, queries
│   ├── models/      # Job domain type
│   ├── config/      # env-var configuration
│   ├── worker/      # 🚧 goroutine pool + processor
│   └── queue/       # 🚧 Kafka producer + consumer
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

| Response | Meaning |
|---|---|
| `202 Accepted` | new job created |
| `200 OK` | job already existed — idempotency hit, returns the stored job |
| `400 Bad Request` | missing `job_id` / `payload` or invalid JSON |
| `500` | server/database error |

### `GET /jobs/{id}`
Fetch a job by ID. → `200 OK` with the job, or `404 Not Found`.

---

## Getting started

### Prerequisites
- Go 1.22+
- Docker + Docker Compose

### 1. Start PostgreSQL
```bash
docker-compose up -d
```
Brings up PostgreSQL 16 (db `jobqueue`, user/password `postgres`) on port `5432`
with a health check.

### 2. Run the API server
```bash
go mod tidy
go run ./cmd/api
```
Listens on `:8080` by default.

### Configuration

All config comes from environment variables (with defaults):

| Variable | Default |
|---|---|
| `DB_HOST` | `localhost` |
| `DB_PORT` | `5432` |
| `DB_USER` | `postgres` |
| `DB_PASSWORD` | `postgres` |
| `DB_NAME` | `jobqueue` |
| `SERVER_PORT` | `8080` |

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

Fetch it back:
```bash
curl -i http://localhost:8080/jobs/550e8400-e29b-41d4-a716-446655440000
# → 200 OK
# {"job_id":"550e8400-...","payload":{"task":"send_email","to":"user@example.com"},
#  "status":"pending","attempt_count":0,"max_retries":3, ...}
```

---

## Roadmap

1. ✅ **Idempotent submission + durable storage** (this feature)
2. 🚧 **Worker pool** — goroutine-based execution that claims `pending` jobs
3. 🚧 **Kafka integration** — producer on submit, consumer feeding the pool
4. 🚧 **Retries & failure handling** — `attempt_count`, `max_retries`, backoff
5. 🚧 **Observability** — metrics, structured logs, dead-letter handling
