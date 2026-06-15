import time
import hashlib
import logging
import requests
from datetime import datetime, timedelta
from source.config import settings
from source.db import save_log_metadata, get_active_alert_rules, log_alert_trigger, get_db_connection
from source.opensearch_client import opensearch_client
from source.kafka_client import kafka_client

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("dls.worker")

# Internal tracker for throttling alerts (prevent spamming the same alert every second)
last_alert_triggered = {}

def get_hash(message: str) -> str:
    """Returns SHA-256 hash of a log message."""
    return hashlib.sha256(message.encode('utf-8')).hexdigest()

def send_alertmanager_alert(rule_name: str, field: str, value: str, count: int, threshold: int):
    """Sends a standardized alert payload to Alertmanager."""
    payload = [
        {
          "labels": {
            "alertname": rule_name,
            "severity": "critical",
            "matching_field": field,
            "matching_value": value
          },
          "annotations": {
            "summary": f"Threshold exceeded for alert '{rule_name}'",
            "description": f"Found {count} matches for {field}={value} in the configured time window. (Threshold: {threshold})"
          }
        }
    ]
    try:
        res = requests.post(settings.ALERTMANAGER_URL, json=payload, timeout=3)
        if res.status_code == 200 or res.status_code == 201:
            logger.info(f"Alert '{rule_name}' successfully sent to Alertmanager.")
        else:
            logger.warning(f"Alertmanager returned code {res.status_code} for alert '{rule_name}': {res.text}")
    except Exception as e:
        logger.error(f"Failed to deliver alert '{rule_name}' to Alertmanager: {e}")

def evaluate_alerts(log_entry: dict):
    """Checks active rules in DB and counts matching logs in the time window to trigger alerts."""
    try:
        active_rules = get_active_alert_rules()
    except Exception as e:
        logger.error(f"Worker failed to load active alert rules from database: {e}")
        return

    now = datetime.utcnow()
    
    for rule in active_rules:
        rule_id = rule["id"]
        rule_name = rule["name"]
        field = rule["query_field"]
        value = rule["query_value"]
        threshold = rule["threshold"]
        window_sec = rule["time_window_sec"]

        # Check if the current incoming log matches the rule's criteria
        # If yes, we check historical counts to evaluate threshold
        log_val = log_entry.get(field) or (log_entry.get("payload", {}) if isinstance(log_entry.get("payload"), dict) else {}).get(field)
        if str(log_val).upper() != str(value).upper():
            continue

        # Throttling check (limit alerts to once per minute per rule)
        last_triggered = last_alert_triggered.get(rule_id)
        if last_triggered and (now - last_triggered) < timedelta(minutes=1):
            continue

        # Count matching logs in OpenSearch or PostgreSQL metadata in the last N seconds
        start_time = (now - timedelta(seconds=window_sec)).isoformat() + "Z"
        
        # Query OpenSearch for matching log count
        matches = opensearch_client.search_logs(
            service_name=value if field == "service_name" else None,
            level=value if field == "level" else None,
            query_string=value if field == "message" else None,
            start_time=start_time,
            limit=1000
        )
        match_count = len(matches)

        if match_count >= threshold:
            logger.warning(f"ALERT TRIGGERED: {rule_name} (Threshold: {threshold}, Found: {match_count})")
            
            # Log in database alert history
            try:
                log_alert_trigger(rule_id, rule_name, match_count, "FIRING")
            except Exception as e:
                logger.error(f"Failed to log alert history to DB: {e}")

            # Send to Prometheus Alertmanager
            send_alertmanager_alert(rule_name, field, value, match_count, threshold)
            
            # Update throttle cache
            last_alert_triggered[rule_id] = now

def process_log(log_entry: dict):
    """Processes, indexes, stores, and evaluates alerts for a single log record."""
    timestamp_str = log_entry.get("@timestamp")
    service_name = log_entry.get("service_name")
    level = log_entry.get("level")
    message = log_entry.get("message", "")
    request_id = log_entry.get("request_id")
    trace_id = log_entry.get("trace_id")
    
    # 1. Parse date for daily indexing index (e.g. logs-2026.06.14)
    try:
        # Standardize ISO timestamps
        dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        index_suffix = dt.strftime("%Y.%m.%d")
    except Exception:
        index_suffix = datetime.utcnow().strftime("%Y.%m.%d")
        dt = datetime.utcnow()

    index_name = f"logs-{index_suffix}"

    # 2. Write metadata record to PostgreSQL
    msg_hash = get_hash(message)
    try:
        save_log_metadata(
            request_id=request_id,
            trace_id=trace_id,
            service_name=service_name,
            level=level,
            message_hash=msg_hash,
            timestamp=dt
        )
    except Exception as e:
        logger.error(f"Failed to write log metadata to DB: {e}")

    # 3. Index log in OpenSearch
    os_success = opensearch_client.index_log(index_name, log_entry)
    if not os_success:
        logger.error(f"Failed to index log in OpenSearch index {index_name}")

    # 4. Evaluate Alert Rules
    evaluate_alerts(log_entry)

def run_worker():
    logger.info("Initializing Log Processing Worker...")
    
    # Wait for database startup
    try:
        conn = get_db_connection()
        conn.close()
    except Exception as e:
        logger.critical(f"Worker could not connect to Postgres database on startup: {e}")
        time.sleep(5)
    
    consumer = kafka_client.get_consumer(group_id="dls-worker-group")
    
    logger.info("Worker started successfully. Awaiting logs...")
    
    try:
        for message in consumer:
            log_data = message.value
            logger.info(f"Consumed log from {log_data.get('service_name')} [{log_data.get('level')}]: {log_data.get('message')[:50]}...")
            try:
                process_log(log_data)
            except Exception as e:
                logger.error(f"Error processing consumed log: {e}")
    except KeyboardInterrupt:
        logger.info("Worker shutting down gracefully...")
    except Exception as e:
        logger.error(f"Fatal worker exception: {e}")

if __name__ == "__main__":
    run_worker()
