import os
import time
import json
import logging
import requests

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("dls.agent")

LOG_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs", "app.log")
INGEST_URL = "http://localhost:8000/api/v1/ingest"
BATCH_SIZE = 10
FLUSH_INTERVAL_SEC = 1.0

def run_agent():
    logger.info(f"Starting Custom Log Agent. Monitoring: {LOG_FILE}")
    
    # Wait for the log file to be created by the generator
    while not os.path.exists(LOG_FILE):
        logger.info("Awaiting creation of application log file...")
        time.sleep(2)

    # Open log file and seek to the end (tail behavior)
    with open(LOG_FILE, "r") as f:
        f.seek(0, os.SEEK_END)
        offset = f.tell()
        
        batch = []
        last_flush = time.time()
        
        try:
            while True:
                # Seek to saved offset
                f.seek(offset)
                line = f.readline()
                
                if line:
                    # Update offset
                    offset = f.tell()
                    line_str = line.strip()
                    if not line_str:
                        continue
                    
                    try:
                        log_data = json.loads(line_str)
                        # Enrich log with Agent properties
                        log_data.setdefault("host_name", "node-1.prod.dls")
                        log_data.setdefault("source", "stdout")
                        batch.append(log_data)
                    except Exception as parse_err:
                        logger.error(f"Error parsing log line: {line_str}. Error: {parse_err}")
                else:
                    # No new line. Sleep briefly.
                    time.sleep(0.1)

                # Check if batch needs flushing
                now = time.time()
                if len(batch) >= BATCH_SIZE or (batch and (now - last_flush) >= FLUSH_INTERVAL_SEC):
                    flush_batch(batch)
                    batch = []
                    last_flush = now

        except KeyboardInterrupt:
            logger.info("Log Agent stopping...")

def flush_batch(batch):
    """Sends batch of logs via POST to the ingestion API. Retries on failure."""
    if not batch:
        return

    logger.info(f"Flushing batch of {len(batch)} logs...")
    
    retries = 3
    while retries > 0:
        try:
            res = requests.post(INGEST_URL, json=batch, timeout=5)
            if res.status_code == 201:
                logger.info(f"Successfully ingested {len(batch)} logs.")
                return
            else:
                logger.error(f"Ingestion API returned error {res.status_code}: {res.text}")
        except Exception as e:
            logger.error(f"Failed to post logs to ingestion API: {e}")
        
        retries -= 1
        logger.info(f"Retrying in 2 seconds... ({retries} retries left)")
        time.sleep(2)
        
    logger.error("Failed to ingest batch after multiple retries. Discarding batch to prevent buffer lock.")

if __name__ == "__main__":
    run_agent()
