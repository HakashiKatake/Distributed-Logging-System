import time
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from source.config import settings

logger = logging.getLogger("dls.db")

def get_db_connection():
    """Returns a PostgreSQL connection with retry logic."""
    retries = 5
    while retries > 0:
        try:
            conn = psycopg2.connect(
                host=settings.DB_HOST,
                port=settings.DB_PORT,
                user=settings.DB_USER,
                password=settings.DB_PASSWORD,
                dbname=settings.DB_NAME
            )
            return conn
        except psycopg2.OperationalError as e:
            logger.warning(f"Database connection failed. Retrying in 2s... Errors remaining: {retries}. Error: {e}")
            retries -= 1
            time.sleep(2)
    raise Exception("Could not connect to the database after several retries.")

def init_db():
    """Initializes tables and seeds default alert rules."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 1. Logs Metadata Table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS logs_metadata (
            id SERIAL PRIMARY KEY,
            request_id VARCHAR(100),
            trace_id VARCHAR(100),
            service_name VARCHAR(100) NOT NULL,
            level VARCHAR(20) NOT NULL,
            message_hash VARCHAR(64),
            timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
            is_archived BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_logs_metadata_timestamp ON logs_metadata(timestamp);
        CREATE INDEX IF NOT EXISTS idx_logs_metadata_request_id ON logs_metadata(request_id);
    """)

    # 2. Alert Rules Table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS alert_rules (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL UNIQUE,
            query_field VARCHAR(50) NOT NULL, -- 'level' or 'service_name' or 'message'
            query_value VARCHAR(100) NOT NULL, -- e.g. 'ERROR', 'payment-service', etc.
            threshold INT NOT NULL, -- threshold count of logs within time window
            time_window_sec INT NOT NULL, -- time window in seconds
            active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
    """)

    # 3. Saved Searches Table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS saved_searches (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            filters JSONB NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
    """)

    # 4. Alert History Table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS alert_history (
            id SERIAL PRIMARY KEY,
            rule_id INT REFERENCES alert_rules(id) ON DELETE CASCADE,
            rule_name VARCHAR(100) NOT NULL,
            trigger_value INT NOT NULL,
            status VARCHAR(20) DEFAULT 'FIRING', -- 'FIRING' or 'RESOLVED'
            triggered_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
    """)

    # Seed default alert rules if table is empty
    cursor.execute("SELECT COUNT(*) FROM alert_rules;")
    if cursor.fetchone()[0] == 0:
        default_rules = [
            ("Critical System Failures", "level", "CRITICAL", 2, 60),
            ("High Error Volume", "level", "ERROR", 5, 60),
            ("Payment Service Failures", "service_name", "payment-service", 3, 120)
        ]
        for name, field, value, thresh, window in default_rules:
            cursor.execute("""
                INSERT INTO alert_rules (name, query_field, query_value, threshold, time_window_sec)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (name) DO NOTHING;
            """, (name, field, value, thresh, window))
            
    conn.commit()
    cursor.close()
    conn.close()
    logger.info("Database initialized successfully.")

# CRUD utilities

def save_log_metadata(request_id, trace_id, service_name, level, message_hash, timestamp):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO logs_metadata (request_id, trace_id, service_name, level, message_hash, timestamp)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING id;
    """, (request_id, trace_id, service_name, level, message_hash, timestamp))
    log_id = cursor.fetchone()[0]
    conn.commit()
    cursor.close()
    conn.close()
    return log_id

def get_unarchived_metadata_before(timestamp):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("""
        SELECT * FROM logs_metadata
        WHERE timestamp < %s AND is_archived = FALSE;
    """, (timestamp,))
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return results

def mark_as_archived(ids):
    if not ids:
        return
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE logs_metadata
        SET is_archived = TRUE
        WHERE id = ANY(%s);
    """, (ids,))
    conn.commit()
    cursor.close()
    conn.close()

def delete_archived_metadata_before(timestamp):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        DELETE FROM logs_metadata
        WHERE timestamp < %s AND is_archived = TRUE;
    """, (timestamp,))
    deleted = cursor.rowcount
    conn.commit()
    cursor.close()
    conn.close()
    return deleted

def get_active_alert_rules():
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("SELECT * FROM alert_rules WHERE active = TRUE;")
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return results

def create_alert_rule(name, query_field, query_value, threshold, time_window_sec):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO alert_rules (name, query_field, query_value, threshold, time_window_sec)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING id;
    """, (name, query_field, query_value, threshold, time_window_sec))
    rule_id = cursor.fetchone()[0]
    conn.commit()
    cursor.close()
    conn.close()
    return rule_id

def log_alert_trigger(rule_id, rule_name, trigger_value, status="FIRING"):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO alert_history (rule_id, rule_name, trigger_value, status)
        VALUES (%s, %s, %s, %s)
        RETURNING id;
    """, (rule_id, rule_name, trigger_value, status))
    history_id = cursor.fetchone()[0]
    conn.commit()
    cursor.close()
    conn.close()
    return history_id

def get_alert_history():
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("SELECT * FROM alert_history ORDER BY triggered_at DESC LIMIT 50;")
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return results

def create_saved_search(name, filters):
    import json
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO saved_searches (name, filters)
        VALUES (%s, %s)
        RETURNING id;
    """, (name, json.dumps(filters)))
    search_id = cursor.fetchone()[0]
    conn.commit()
    cursor.close()
    conn.close()
    return search_id

def get_saved_searches():
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("SELECT * FROM saved_searches ORDER BY created_at DESC;")
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return results
