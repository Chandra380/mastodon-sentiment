# Real-Time Sentiment Pipeline — Step-by-Step Roadmap

Production-style, incremental build. Each step is testable before moving on.

---

## Current state

| Component | Status | Notes |
|-----------|--------|--------|
| Kafka + Kafka UI | ✅ Done | `docker-compose up -d` — broker on 9092, UI on 8080 |
| Medallion layout | ✅ Done | `data/bronze`, `data/silver`, `data/gold` |
| Producer (Mastodon → Kafka) | ⬜ Not started | Next up |
| Bronze ingestion | ⬜ Not started | Kafka → raw landing |
| Silver (clean + sentiment) | ⬜ Not started | Spark or Python streaming |
| Gold (aggregates) | ⬜ Not started | dbt + DuckDB |
| Dashboard | ⬜ Not started | Sentiment viz |
| Airflow | ⬜ Not started | Orchestration / backfill |

---

## Step 1 — Kafka + infrastructure ✅

- [x] Docker Compose: Kafka (KRaft), Kafka UI
- [x] `.env.example` with `KAFKA_BROKER`, `KAFKA_TOPIC`
- [x] Topic: `mastodon-posts` (create on first use or via init)

---

## Step 2 — Mastodon → Kafka producer (NEXT)

**Goal:** Ingest Mastodon posts in real time and publish to Kafka.

- [ ] Python producer (e.g. `producers/mastodon_producer.py`)
- [ ] Use Mastodon streaming API or polling; normalize payload (id, content, created_at, account, etc.)
- [ ] Env: `MASTODON_INSTANCE`, `MASTODON_ACCESS_TOKEN`
- [ ] Create topic if not exists; backpressure/retry; graceful shutdown
- [ ] Optional: dockerize producer or run via `docker-compose` service

**Production notes:** Structured logging, health endpoint or heartbeat, idempotent/retry, rate limiting for API.

---

## Step 3 — Bronze: raw landing

**Goal:** Persist every event from Kafka into Bronze (immutable raw layer).

- [ ] Consumer that reads `mastodon-posts` and writes to `data/bronze/` (e.g. JSON/Parquet by date or hour)
- [ ] Or Spark Structured Streaming job writing to bronze path
- [ ] Schema-on-read; no transformation except partitioning (e.g. by date)

**Production notes:** Checkpointing/offsets so no duplicate or lost reads; partition by date for retention and downstream use.

---

## Step 4 — Silver: clean + sentiment

**Goal:** Clean, dedupe, and add sentiment; output to Silver.

- [ ] Read from Bronze (or Kafka) → clean (nulls, encoding, language filter if needed)
- [ ] Dedupe by post id
- [ ] Sentiment model (e.g. Hugging Face `transformers` or a small API) → score or label
- [ ] Write to `data/silver/` (Parquet preferred)

**Production notes:** Version the model; optional A/B or fallback; schema registry or explicit schema for Silver.

---

## Step 5 — Gold: aggregates for dashboards

**Goal:** Pre-aggregated metrics for fast dashboards.

- [ ] dbt project in `dbt/` reading Silver (e.g. from DuckDB)
- [ ] Models: time-series sentiment (by hour/day), by hashtag/account, volume, etc.
- [ ] Output to `data/gold/` or DuckDB

**Production notes:** Incremental models where possible; tests on uniqueness and freshness.

---

## Step 6 — Dashboard

**Goal:** Interactive view of sentiment over time and by dimension.

- [ ] App in `dashboard/` (e.g. Streamlit, Dash, or React + API)
- [ ] Read from Gold / DuckDB; simple filters (time range, hashtag, account)

---

## Step 7 — Orchestration (Airflow)

**Goal:** Schedule batch backfills, dbt runs, and optional data quality checks.

- [ ] Airflow in Docker; DAGs in `airflow/dags/`
- [ ] DAGs: Bronze→Silver→Gold batch run, dbt run, alerting on failures

---

## Quick reference

- **Kafka:** `localhost:9092` — topic `mastodon-posts`
- **Kafka UI:** http://localhost:8080
- **Env:** Copy `.env.example` to `.env` and set `MASTODON_*` when building the producer

Pick up from **Step 2 (Producer)** when continuing the build.
