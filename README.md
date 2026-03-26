# Mastodon Sentiment Analysis Pipeline

Real-time brand sentiment tracking using Mastodon API, Kafka, Spark, dbt, DuckDB, and Airflow.

## Architecture (Medallion)

- **Bronze** → Raw data landing (Mastodon posts from Kafka)
- **Silver** → Cleaned, enriched, deduplicated
- **Gold** → Analytical aggregates for dashboards

## Tech Stack

- Apache Kafka (streaming buffer)
- Apache Spark (streaming + batch processing)
- DuckDB (analytical warehouse)
- Airflow (orchestration)
- Docker Compose (containerization)

## Project Structure

```
mastodon-sentiment/
├── data/                 # Persistent data (Bronze/Silver/Gold files)
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── docker-compose.yml
├── dbt/                  # dbt project (transformations)
├── airflow/              # Airflow DAGs (orchestration)
├── spark/                # Spark streaming jobs
├── producers/            # Mastodon → Kafka producer
└── dashboard/            # Sentiment dashboard
```

## Getting Started

1. Copy `.env.example` to `.env`. For the producer you’ll need Mastodon credentials — see [docs/MASTODON_CREDENTIALS.md](docs/MASTODON_CREDENTIALS.md).
2. Run `docker-compose up -d`
3. See **[PROJECT_ROADMAP.md](docs/PROJECT_ROADMAP.md)** for the full step-by-step build (production-style). Next up: **Step 2 — Mastodon → Kafka producer**.

**Pushing to GitHub:** [docs/GITHUB_SETUP.md](docs/GITHUB_SETUP.md) — one-time repo creation, remote, and CI/CD (GitHub Actions).
