"""
consumer_dowdle.py

Consume JSON messages from Kafka (topic: buzzline_dowdle, group: buzz_dowdle).
Track keyword mentions in real time.

Processed results:
- Update keyword counts in SQLite database (dowdle.sqlite).
- Update keyword counts in JSON file (dowdle_live.json).
"""

#####################################
# Imports
#####################################

import json
import os
import pathlib
import sqlite3
import sys
from kafka import KafkaConsumer

# Local imports
from utils.utils_logger import logger
from utils.utils_producer import verify_services, is_topic_available
from utils.utils_consumer import create_kafka_consumer

#####################################
# Paths
#####################################

BASE_DIR = pathlib.Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
SQLITE_PATH = DATA_DIR / "dowdle.sqlite"
JSON_PATH = DATA_DIR / "dowdle_live.json"

TOPIC = "buzzline_dowdle"
GROUP_ID = "buzz_dowdle"
KAFKA_URL = "127.0.0.1:9092"


#####################################
# Database Setup
#####################################

def init_db(db_path: pathlib.Path):
    """Initialize SQLite database with a table for keyword counts."""
    logger.info(f"Initializing SQLite DB at {db_path}")
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS keyword_counts (
            keyword TEXT PRIMARY KEY,
            count INTEGER NOT NULL
        )
        """
    )
    conn.commit()
    conn.close()


def update_keyword_count(db_path: pathlib.Path, keyword: str):
    """Increment keyword count in SQLite DB."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO keyword_counts (keyword, count)
        VALUES (?, 1)
        ON CONFLICT(keyword) DO UPDATE SET count = count + 1
        """,
        (keyword,),
    )
    conn.commit()
    conn.close()


#####################################
# JSON File Handling
#####################################

def update_json_file(json_path: pathlib.Path, keyword: str):
    """Increment keyword count in JSON file."""
    if json_path.exists():
        with open(json_path, "r") as f:
            counts = json.load(f)
    else:
        counts = {}

    counts[keyword] = counts.get(keyword, 0) + 1

    with open(json_path, "w") as f:
        json.dump(counts, f, indent=4)


#####################################
# Process Single Message
#####################################

def process_message(message: dict):
    """Process a single Kafka message and update stores."""
    keyword = message.get("keyword_mentioned")
    if not keyword:
        logger.warning("Message missing keyword_mentioned field.")
        return

    logger.info(f"Processing message: keyword={keyword}")

    # Update SQLite and JSON
    update_keyword_count(SQLITE_PATH, keyword)
    update_json_file(JSON_PATH, keyword)


#####################################
# Kafka Consumer
#####################################

def consume_messages():
    """Consume messages from Kafka and process one at a time."""
    logger.info("Step 1. Verify Kafka Services.")
    verify_services()

    logger.info("Step 2. Create Kafka consumer.")
    consumer = create_kafka_consumer(
        TOPIC,
        GROUP_ID,
        value_deserializer_provided=lambda x: json.loads(x.decode("utf-8")),
    )

    logger.info("Step 3. Verify topic exists.")
    is_topic_available(TOPIC)

    logger.info("Step 4. Process messages in real time.")
    for record in consumer:
        process_message(record.value)


#####################################
# Main
#####################################

def main():
    logger.info("Starting consumer_dowdle.")
    if SQLITE_PATH.exists():
        SQLITE_PATH.unlink()
        logger.info("Deleted old database.")

    if JSON_PATH.exists():
        JSON_PATH.unlink()
        logger.info("Deleted old JSON file.")

    init_db(SQLITE_PATH)
    consume_messages()


if __name__ == "__main__":
    main()
