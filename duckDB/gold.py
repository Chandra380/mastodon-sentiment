"""
Gold layer: star schema for dashboard.

Read from silver_enriched (Silver + sentiment from sentiment_batch.py).
If you skip the sentiment batch, point SILVER_PARQUET_PATH to data/silver and
ensure Gold handles missing sentiment_score (e.g. null).

Silver (stream)     → data/silver/event_date=*/*
Sentiment (batch)   → data/silver_enriched/event_date=*/*  (same schema + sentiment_score)
Gold reads          → data/silver_enriched/*/* by default

Star schema:
  - dim_authors   : one row per author (author_id)
  - dim_date      : one row per date (for time-based filters/aggregation)
  - dim_tags      : one row per hashtag
  - fact_posts    : one row per post (grain: post); links to dim_authors, dim_date
  - fact_post_tags: bridge post <-> tag (for trending tags, sentiment by tag)
"""
import os

import duckdb

# Default: read enriched Silver (with sentiment). Use data/silver/*/* if no sentiment batch.
SILVER_PATH = os.getenv("SILVER_PARQUET_PATH", "data/silver_enriched/*/*.parquet")
DB_PATH = os.getenv("DUCKDB_PATH", "data/gold/mastodon_analytics.db")


def build_gold_layer() -> None:
    print(f"Connecting to DuckDB: {DB_PATH}")
    con = duckdb.connect(database=DB_PATH)

    # View over latest Silver Parquet (no copy; reads files on query)
    con.execute(f"""
        CREATE OR REPLACE VIEW v_silver_posts AS
        SELECT * FROM read_parquet('{SILVER_PATH}')
    """)

    # ---- Dimensions ----

    # dim_authors: one row per author (Silver columns: author_id, author_acct, author_name, author_followers_count, ...)
    print("Building dim_authors...")
    con.execute("""
        CREATE OR REPLACE TABLE dim_authors AS
        SELECT DISTINCT
            author_id,
            author_acct,
            author_name,
            author_followers_count,
            author_following_count,
            author_statuses_count,
            is_bot
        FROM v_silver_posts
        WHERE author_id IS NOT NULL
    """)

    # dim_date: one row per event_date (for dashboard date filters / time hierarchy)
    print("Building dim_date...")
    con.execute("""
        CREATE OR REPLACE TABLE dim_date AS
        SELECT DISTINCT
            event_date,
            year(event_date) AS year,
            month(event_date) AS month,
            day(event_date) AS day
        FROM v_silver_posts
        WHERE event_date IS NOT NULL
    """)

    # dim_tags: one row per hashtag (from unnest of tags array)
    print("Building dim_tags...")
    con.execute("""
        CREATE OR REPLACE TABLE dim_tags AS
        SELECT DISTINCT unnest(tags) AS tag_name
        FROM v_silver_posts
        WHERE len(tags) > 0
    """)

    # ---- Facts ----

    # fact_posts: one row per post; FKs to dim_authors, dim_date
    print("Building fact_posts...")
    con.execute("""
        CREATE OR REPLACE TABLE fact_posts AS
        SELECT
            post_id,
            author_id,
            event_date,
            created_at,
            application_name,
            sentiment_score,
            reblogs_count,
            favourites_count,
            replies_count,
            quotes_count,
            visibility,
            language
        FROM v_silver_posts
    """)

    # fact_post_tags: bridge (post_id, tag_name, event_date) for "trending by tag" / "sentiment by tag"
    print("Building fact_post_tags...")
    con.execute("""
        CREATE OR REPLACE TABLE fact_post_tags AS
        SELECT
            post_id,
            unnest(tags) AS tag_name,
            event_date,
            sentiment_score
        FROM v_silver_posts
        WHERE len(tags) > 0
    """)

    print("Gold layer (star schema) built successfully.")

    # Summary
    posts = con.execute("SELECT count(*) FROM fact_posts").fetchone()[0]
    authors = con.execute("SELECT count(*) FROM dim_authors").fetchone()[0]
    tag_rows = con.execute("SELECT count(*) FROM fact_post_tags").fetchone()[0]
    print(f"Summary: {posts} posts, {authors} authors, {tag_rows} post-tag rows.")
    con.close()


if __name__ == "__main__":
    build_gold_layer()
