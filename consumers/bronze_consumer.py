import json
import logging
import os
import signal
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from dotenv import load_dotenv
from kafka import KafkaConsumer


log = logging.getLogger("bronze_consumer")


def setup_logging() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )


def load_config() -> Dict[str, str]:
    load_dotenv()

    required = ["KAFKA_BROKER", "KAFKA_TOPIC"]
    cfg: Dict[str, str] = {}
    for var in required:
        value = os.getenv(var)
        if not value:
            raise RuntimeError(f"Missing required environment variable: {var}")
        cfg[var] = value

    bronze_dir = os.getenv("BRONZE_DIR", "data/bronze")
    cfg["BRONZE_DIR"] = bronze_dir
    return cfg


class GracefulKiller:
    def __init__(self) -> None:
        self._stop = False
        signal.signal(signal.SIGINT, self._handle)
        signal.signal(signal.SIGTERM, self._handle)

    @property
    def stop(self) -> bool:
        return self._stop

    def _handle(self, signum, frame) -> None:  # type: ignore[override]
        log.info("Received signal %s, stopping consumer...", signum)
        self._stop = True


def get_bronze_file_path(base_dir: str) -> Path:
    now = datetime.now(timezone.utc)
    date_str = now.strftime("%Y-%m-%d")
    dir_path = Path(base_dir) / date_str
    dir_path.mkdir(parents=True, exist_ok=True)
    file_path = dir_path / f"mastodon_posts_{date_str}.jsonl"
    return file_path


def run() -> None:
    setup_logging()

    try:
        cfg = load_config()
    except RuntimeError as exc:
        log.error("Config error: %s", exc)
        sys.exit(1)

    broker = cfg["KAFKA_BROKER"]
    topic = cfg["KAFKA_TOPIC"]
    bronze_dir = cfg["BRONZE_DIR"]

    log.info(
        "Starting Bronze consumer; broker=%s, topic=%s, bronze_dir=%s",
        broker,
        topic,
        bronze_dir,
    )

    killer = GracefulKiller()

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=broker,
        group_id=os.getenv("BRONZE_CONSUMER_GROUP", "mastodon-bronze-consumer"),
        enable_auto_commit=True,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda v: v.decode("utf-8") if v is not None else None,
    )

    current_file: Optional[Path] = None
    fh = None

    try:
        for message in consumer:
            if killer.stop:
                break

            # Rotate file daily based on UTC date
            target_path = get_bronze_file_path(bronze_dir)
            if current_file != target_path:
                if fh is not None:
                    fh.flush()
                    fh.close()
                current_file = target_path
                fh = open(current_file, "a", encoding="utf-8")
                log.info("Writing Bronze data to %s", current_file)

            record: Dict[str, Any] = {
                "kafka": {
                    "topic": message.topic,
                    "partition": message.partition,
                    "offset": message.offset,
                    "timestamp": message.timestamp,
                    "key": message.key,
                },
                "value": message.value,
            }
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")
    finally:
        if fh is not None:
            fh.flush()
            fh.close()
        consumer.close()
        log.info("Bronze consumer stopped.")


if __name__ == "__main__":
    run()

