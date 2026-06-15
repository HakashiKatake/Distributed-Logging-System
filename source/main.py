import uuid
import time
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
from fastapi import FastAPI, HTTPException, Query, Response, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST

from source.config import settings
from source.db import (
    init_db, save_log_metadata, get_active_alert_rules, 
    create_alert_rule, get_alert_history, create_saved_search, 
    get_saved_searches, get_db_connection, log_alert_trigger
)
from source.opensearch_client import opensearch_client
from source.s3_client import s3_client
from source.kafka_client import kafka_client

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("dls.api")

app = FastAPI(
    title="Distributed Logging System API",
    description="Production-style observability log collection, ingestion, search, and monitoring API.",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Prometheus metrics setup
LOGS_INGESTED = Counter("logs_ingested_total", "Total number of ingested logs", ["service", "level"])
INGESTION_LATENCY = Histogram("ingestion_latency_seconds", "Latency of the log ingestion endpoint in seconds")
KAFKA_QUEUE_LAG_GAUGE = Gauge("kafka_queue_lag", "Pending messages in in-memory queue fallback")

# Ingestion models
class LogPayload(BaseModel):
    timestamp: Optional[str] = None
    service_name: str = Field(..., example="auth-service")
    host_name: str = Field(..., example="node-1.prod.dls")
    level: str = Field("INFO", example="INFO")
    message: str = Field(..., example="User login successful for user_id=123")
    source: str = Field("stdout", example="stdout")
    request_id: Optional[str] = None
    trace_id: Optional[str] = None
    payload: Optional[Dict[str, Any]] = {}

class AlertRuleCreate(BaseModel):
    name: str
    query_field: str
    query_value: str
    threshold: int
    time_window_sec: int

class SavedSearchCreate(BaseModel):
    name: str
    filters: Dict[str, Any]

@app.on_event("startup")
def startup_event():
    logger.info("Starting up Distributed Logging System API...")
    try:
        init_db()
    except Exception as e:
        logger.error(f"Startup DB initialization failed: {e}")

@app.get("/health", status_code=status.HTTP_200_OK)
def health_check():
    """Health check endpoint validating connectivity to all backend systems."""
    health_status = {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "dependencies": {
            "postgresql": "healthy",
            "opensearch": "healthy",
            "s3": "healthy",
            "kafka": "healthy"
        }
    }
    
    # Check Postgres
    try:
        conn = get_db_connection()
        conn.close()
    except Exception as e:
        health_status["status"] = "degraded"
        health_status["dependencies"]["postgresql"] = f"unhealthy: {e}"
        
    # Check OpenSearch
    if not opensearch_client.client:
        health_status["status"] = "degraded"
        health_status["dependencies"]["opensearch"] = "unhealthy: not connected"
        
    # Check S3
    if not s3_client.s3:
        health_status["status"] = "degraded"
        health_status["dependencies"]["s3"] = "unhealthy: not connected"
        
    # Check Kafka (if mock, marked as healthy fallback)
    if kafka_client._is_fallback:
        health_status["dependencies"]["kafka"] = "healthy: fallback in-memory mode active"

    if health_status["status"] != "healthy":
        raise HTTPException(status_code=503, detail=health_status)
    return health_status

@app.post("/api/v1/ingest", status_code=status.HTTP_201_CREATED)
@INGESTION_LATENCY.time()
def ingest_logs(logs: List[LogPayload]):
    """Ingests batches of structured JSON logs, enriches missing fields, and streams to Kafka."""
    processed_count = 0
    errors_count = 0
    
    for log in logs:
        # Auto-enrich missing fields
        timestamp = log.timestamp or datetime.utcnow().isoformat() + "Z"
        request_id = log.request_id or str(uuid.uuid4())
        trace_id = log.trace_id or request_id
        
        log_entry = {
            "@timestamp": timestamp,
            "service_name": log.service_name,
            "host_name": log.host_name,
            "level": log.level.upper(),
            "message": log.message,
            "source": log.source,
            "request_id": request_id,
            "trace_id": trace_id,
            "payload": log.payload
        }
        
        # Publish to Kafka buffer
        success = kafka_client.publish_log(log_entry)
        if success:
            LOGS_INGESTED.labels(service=log.service_name, level=log.level.upper()).inc()
            processed_count += 1
        else:
            errors_count += 1

    # Update in-memory queue gauge metrics
    KAFKA_QUEUE_LAG_GAUGE.set(kafka_client.get_fallback_queue_size())

    return {
        "status": "success",
        "ingested_count": processed_count,
        "failed_count": errors_count
    }

@app.get("/api/v1/search")
def search_logs(
    service: Optional[str] = Query(None, description="Filter logs by service name"),
    level: Optional[str] = Query(None, description="Filter logs by severity level (e.g. ERROR)"),
    request_id: Optional[str] = Query(None, description="Filter logs by request ID for distributed tracing"),
    query: Optional[str] = Query(None, description="Full-text query string search on log message"),
    start_time: Optional[str] = Query(None, description="Start date ISO-8601 format"),
    end_time: Optional[str] = Query(None, description="End date ISO-8601 format"),
    limit: int = Query(50, ge=1, le=1000, description="Max logs returned")
):
    """Searches hot indexed logs in OpenSearch using filters and full-text keyword queries."""
    results = opensearch_client.search_logs(
        service_name=service,
        level=level,
        start_time=start_time,
        end_time=end_time,
        request_id=request_id,
        query_string=query,
        limit=limit
    )
    return {
        "count": len(results),
        "results": results
    }

# Saved Searches Endpoints

@app.post("/api/v1/searches/saved", status_code=status.HTTP_201_CREATED)
def save_search_query(payload: SavedSearchCreate):
    """Saves a recurring search query setup in PostgreSQL."""
    try:
        search_id = create_saved_search(payload.name, payload.filters)
        return {"id": search_id, "name": payload.name, "status": "saved"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/api/v1/searches/saved")
def list_saved_searches():
    """Lists all saved searches from PostgreSQL."""
    return get_saved_searches()

# Alerting Management Endpoints

@app.post("/api/v1/alerts/rules", status_code=status.HTTP_201_CREATED)
def create_rule(payload: AlertRuleCreate):
    """Creates a new log-counting alert rule in PostgreSQL."""
    try:
        rule_id = create_alert_rule(
            payload.name, 
            payload.query_field, 
            payload.query_value, 
            payload.threshold, 
            payload.time_window_sec
        )
        return {"id": rule_id, "name": payload.name, "status": "created"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/api/v1/alerts/rules")
def list_alert_rules():
    """Lists all active alert rules in the system."""
    return get_active_alert_rules()

@app.get("/api/v1/alerts/history")
def list_alert_history():
    """Exposes historical triggered alert list."""
    return get_alert_history()

@app.post("/api/v1/alerts/webhook", status_code=status.HTTP_200_OK)
def alertmanager_webhook(alert_payload: Dict[str, Any]):
    """Receiver endpoint for Alertmanager webhook deliveries. Logs fired alerts in database."""
    logger.warning(f"ALERT RECEIVED FROM ALERTMANAGER: {alert_payload}")
    # Extract details
    alerts = alert_payload.get("alerts", [])
    for alert in alerts:
        labels = alert.get("labels", {})
        annotations = alert.get("annotations", {})
        status = alert.get("status", "firing").upper()
        
        rule_name = labels.get("alertname", "Unknown Rule")
        message = annotations.get("summary", annotations.get("description", "Alert fired"))
        
        # Log to db history (associate with rule 0 if not custom matched)
        log_alert_trigger(
            rule_id=None,
            rule_name=f"{rule_name}: {message}",
            trigger_value=0,
            status=status
        )
    return {"status": "alert_received"}

# Prometheus metrics endpoint
@app.get("/metrics")
def get_metrics():
    """Exposes scraping metrics compatible with Prometheus server."""
    # Sync in-memory lag metric
    KAFKA_QUEUE_LAG_GAUGE.set(kafka_client.get_fallback_queue_size())
    
    return Response(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST
    )
