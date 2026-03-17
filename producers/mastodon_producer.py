import json
import logging
import os
import signal
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError


log = logging.getLogger("mastodon_producer")


def setup_logging() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )


def load_config() -> Dict[str, str]:
    """
    Load configuration from environment variables.

    We call load_dotenv() so running `python producers/mastodon_producer.py`
    locally will pick up values from .env. In Docker, env vars will be injected
    by the container runtime.
    """
    load_dotenv()

    required_vars = ["KAFKA_BROKER", "KAFKA_TOPIC", "MASTODON_INSTANCE", "MASTODON_ACCESS_TOKEN"]
    cfg: Dict[str, str] = {}

    for var in required_vars:
        value = os.getenv(var)
        if not value:
            raise RuntimeError(f"Missing required environment variable: {var}")
        cfg[var] = value

    return cfg


def build_kafka_producer(bootstrap_servers: str) -> KafkaProducer:
    """
    Create a Kafka producer with sane defaults for a small pipeline.
    """
    log.info("Creating Kafka producer for %s", bootstrap_servers)
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda v: v.encode("utf-8") if v is not None else None,
        retries=5,
        linger_ms=5,
        acks="all",
    )
    return producer


def fetch_public_timeline(
    instance: str,
    access_token: str,
    since_id: Optional[str] = None,
    limit: int = 40,
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """
    Poll the local public timeline.

    This is a pragmatic starting point: we poll rather than using streaming,
    and keep track of since_id to avoid re-sending the same posts.
    """
    headers = {"Authorization": f"Bearer {access_token}"}
    params: Dict[str, Any] = {"limit": limit, "local": "true"}
    if since_id:
        params["since_id"] = since_id

    url = f"{instance.rstrip('/')}/api/v1/timelines/public"
    resp = requests.get(url, headers=headers, params=params, timeout=10)
    resp.raise_for_status()

    statuses = resp.json()
    if not isinstance(statuses, list):
        log.warning("Unexpected response shape from Mastodon: %s", type(statuses))
        return [], since_id

    # Mastodon returns newest first; track the highest id we have seen
    new_since_id = since_id
    for status in statuses:
        sid = status.get("id")
        if isinstance(sid, str):
            if new_since_id is None or sid > new_since_id:
                new_since_id = sid

    return statuses, new_since_id


class GracefulKiller:
    """
    Handle SIGINT/SIGTERM for clean shutdown.
    """

    def __init__(self) -> None:
        self._stop = False
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    @property
    def stop(self) -> bool:
        return self._stop

    def _handle_signal(self, signum, frame) -> None:  # type: ignore[override]
        log.info("Received signal %s, shutting down gracefully...", signum)
        self._stop = True


def run() -> None:
    setup_logging()

    try:
        cfg = load_config()
    except RuntimeError as exc:
        log.error("Configuration error: %s", exc)
        sys.exit(1)

    broker = cfg["KAFKA_BROKER"]
    topic = cfg["KAFKA_TOPIC"]
    instance = cfg["MASTODON_INSTANCE"]
    token = cfg["MASTODON_ACCESS_TOKEN"]

    producer = build_kafka_producer(broker)
    killer = GracefulKiller()

    poll_interval_seconds = int(os.getenv("POLLER_INTERVAL_SECONDS", "5"))
    since_id: Optional[str] = None

    log.info(
        "Starting Mastodon → Kafka producer; instance=%s, topic=%s, interval=%ss",
        instance,
        topic,
        poll_interval_seconds,
    )

    try:
        while not killer.stop:
            try:
                statuses, since_id = fetch_public_timeline(
                    instance=instance,
                    access_token=token,
                    since_id=since_id,
                )
            except requests.RequestException as exc:
                log.warning("Error fetching from Mastodon: %s", exc)
                time.sleep(poll_interval_seconds)
                continue

            if not statuses:
                log.debug("No new statuses; sleeping %ss", poll_interval_seconds)
                time.sleep(poll_interval_seconds)
                continue

            log.info("Fetched %d new statuses from Mastodon", len(statuses))

            for status in statuses:
                key = str(status.get("id", ""))
                payload = {
                    "id": status.get("id"),
                    "created_at": status.get("created_at"),
                    "content": status.get("content"),
                    "account": {
                        "id": status.get("account", {}).get("id"),
                        "acct": status.get("account", {}).get("acct"),
                        "display_name": status.get("account", {}).get("display_name"),
                    },
                    "visibility": status.get("visibility"),
                    "language": status.get("language"),
                    "raw": status,
                }
                try:
                    future = producer.send(topic, key=key, value=payload)
                    future.get(timeout=10)
                except KafkaError as exc:
                    log.error("Failed to send message to Kafka: %s", exc)
                    # We continue; retries are handled by producer config

            producer.flush()
            time.sleep(poll_interval_seconds)
    finally:
        log.info("Flushing and closing Kafka producer...")
        try:
            producer.flush(timeout=10)
        except Exception:  # noqa: BLE001
            pass
        producer.close()
        log.info("Producer stopped.")


if __name__ == "__main__":
    run()

