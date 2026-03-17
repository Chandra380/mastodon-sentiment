import duckdb
import os

# Configuration
SILVER_PARQUET_PATH = "data/silver/parquet/*/*.parquet"
DB_PATH = "data/mastodon_analytics.db"

def sync_silver_to_duckdb():
    print(f"Connecting to DuckDB at {DB_PATH}...")
    con = duckdb.connect(database=DB_PATH)

    # 1. Create the Main Silver Table (if not exists)
    # We use a view to easily 'upsert' new data
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS silver_posts AS 
        SELECT * FROM read_parquet('{SILVER_PARQUET_PATH}') LIMIT 0;
    """)

    # 2. Incremental Load (Avoid Duplicates)
    # We only insert rows where post_id doesn't already exist in DuckDB
    print("Ingesting new posts from Parquet...")
    con.execute(f"""
        INSERT INTO silver_posts
        SELECT * FROM read_parquet('{SILVER_PARQUET_PATH}')
        WHERE post_id NOT IN (SELECT post_id FROM silver_posts);
    """)

    # 3. Refresh the Trending Tags (Gold-ready View/Table)
    # We recreate this because tags can change/grow quickly
    print("Refreshing Trending Tags table...")
    con.execute("""
        CREATE OR REPLACE TABLE gold_trending_tags AS
        SELECT 
            post_id, 
            UNNEST(tags) AS tag_name, 
            event_date,
            count(*) OVER(PARTITION BY UNNEST(tags)) as tag_frequency
        FROM silver_posts;
    """)

    row_count = con.execute("SELECT count(*) FROM silver_posts").fetchone()[0]
    print(f"Success! Total posts in DuckDB: {row_count}")
    con.close()

if __name__ == "__main__":
    sync_silver_to_duckdb()
