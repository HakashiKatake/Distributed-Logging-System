import os
import sys
from pathlib import Path

# Load simple dotenv implementation to avoid external library reliance
env_path = Path(__file__).resolve().parent.parent / ".env"
if env_path.exists():
    with open(env_path, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, val = line.split("=", 1)
            # Remove quotes if present
            val = val.strip().strip("'\"")
            os.environ.setdefault(key.strip(), val)

class Settings:
    # DB
    DB_HOST: str = os.getenv("DB_HOST", "localhost")
    DB_PORT: int = int(os.getenv("DB_PORT", 5432))
    DB_USER: str = os.getenv("DB_USER", "postgres")
    DB_PASSWORD: str = os.getenv("DB_PASSWORD", "postgres")
    DB_NAME: str = os.getenv("DB_NAME", "logging_db")

    # OpenSearch
    OPENSEARCH_HOST: str = os.getenv("OPENSEARCH_HOST", "localhost")
    OPENSEARCH_PORT: int = int(os.getenv("OPENSEARCH_PORT", 9200))
    OPENSEARCH_USER: str = os.getenv("OPENSEARCH_USER", "admin")
    OPENSEARCH_PASSWORD: str = os.getenv("OPENSEARCH_PASSWORD", "AdminPassword123!")

    # Kafka
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
    KAFKA_TOPIC: str = os.getenv("KAFKA_TOPIC", "distributed-logs")

    # S3
    S3_ENDPOINT: str = os.getenv("S3_ENDPOINT", "http://localhost:9000")
    S3_ACCESS_KEY: str = os.getenv("S3_ACCESS_KEY", "minioadmin")
    S3_SECRET_KEY: str = os.getenv("S3_SECRET_KEY", "minioadmin")
    S3_BUCKET: str = os.getenv("S3_BUCKET", "cold-logs-archive")

    # Alertmanager
    ALERTMANAGER_URL: str = os.getenv("ALERTMANAGER_URL", "http://localhost:9093/api/v2/alerts")

    # Ports
    INGESTION_PORT: int = int(os.getenv("INGESTION_PORT", 8000))
    SEARCH_PORT: int = int(os.getenv("SEARCH_PORT", 8000))

    # Retention
    HOT_RETENTION_MINUTES: int = int(os.getenv("HOT_RETENTION_MINUTES", 5))
    COLD_RETENTION_MINUTES: int = int(os.getenv("COLD_RETENTION_MINUTES", 15))

settings = Settings()
