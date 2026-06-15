import json
import time
import logging
from datetime import datetime, timedelta
from source.config import settings
from source.db import (
    get_unarchived_metadata_before, mark_as_archived, 
    delete_archived_metadata_before, get_db_connection
)
from source.opensearch_client import opensearch_client
from source.s3_client import s3_client

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("dls.retention")

def run_retention_cycle():
    """Runs a single cycle of hot log archiving and cold log purging."""
    logger.info("Starting Retention Management cycle...")
    now = datetime.utcnow()

    # --- Phase 1: Archive Hot Logs to Cold Storage (MinIO) ---
    hot_cutoff = now - timedelta(minutes=settings.HOT_RETENTION_MINUTES)
    hot_cutoff_str = hot_cutoff.isoformat() + "Z"
    
    logger.info(f"Checking for logs older than hot retention threshold: {settings.HOT_RETENTION_MINUTES}m (Cutoff: {hot_cutoff_str})")

    # Fetch logs from OpenSearch
    logs_to_archive = opensearch_client.get_logs_before(hot_cutoff_str)
    
    if logs_to_archive:
        logger.info(f"Found {len(logs_to_archive)} logs in OpenSearch ready for cold archiving.")
        
        # Serialize and upload to MinIO S3
        timestamp_slug = now.strftime("%Y%m%d_%H%M%S")
        archive_key = f"archive/logs_{timestamp_slug}.json"
        
        archive_data = json.dumps(logs_to_archive, indent=2)
        upload_success = s3_client.upload_archive(archive_key, archive_data)
        
        if upload_success:
            # Mark log metadata as archived in PostgreSQL
            try:
                # Query Postgres to match IDs
                # We can load unarchived DB metadata entries before the cutoff
                db_entries = get_unarchived_metadata_before(hot_cutoff)
                ids_to_mark = [entry["id"] for entry in db_entries]
                
                if ids_to_mark:
                    mark_as_archived(ids_to_mark)
                    logger.info(f"Marked {len(ids_to_mark)} log metadata rows as archived in PostgreSQL.")
            except Exception as db_err:
                logger.error(f"Failed to update PostgreSQL metadata archive flags: {db_err}")

            # Delete logs from OpenSearch
            deleted_count = opensearch_client.delete_logs_before(hot_cutoff_str)
            logger.info(f"Purged {deleted_count} logs from OpenSearch hot storage.")
        else:
            logger.error("Failed to archive logs to S3. Skipping hot storage deletion.")
    else:
        logger.info("No logs require archiving at this time.")

    # --- Phase 2: Enforce Cold Retention Policy (Delete old S3 Archives and Postgres Metadata) ---
    cold_cutoff = now - timedelta(minutes=settings.COLD_RETENTION_MINUTES)
    logger.info(f"Checking for cold archives exceeding retention threshold: {settings.COLD_RETENTION_MINUTES}m")

    # List objects in MinIO bucket
    archives = s3_client.list_archives()
    purged_archives_count = 0
    
    for obj in archives:
        key = obj.get("Key", "")
        last_modified = obj.get("LastModified") # offset-naive or offset-aware depending on boto3 response
        
        if last_modified:
            # Make cold_cutoff offset-aware to match boto3 datetime response
            last_modified_naive = last_modified.replace(tzinfo=None)
            if last_modified_naive < cold_cutoff:
                logger.warning(f"Archive '{key}' has expired (Last Modified: {last_modified_naive}). Purging.")
                success = s3_client.delete_archive(key)
                if success:
                    purged_archives_count += 1

    if purged_archives_count > 0:
        logger.info(f"Purged {purged_archives_count} expired cold log archives from S3.")
    else:
        logger.info("No expired cold archives found.")

    # Delete corresponding metadata from PostgreSQL
    try:
        deleted_db_rows = delete_archived_metadata_before(cold_cutoff)
        if deleted_db_rows > 0:
            logger.info(f"Deleted {deleted_db_rows} expired metadata records from PostgreSQL.")
    except Exception as db_err:
        logger.error(f"Failed to clean database metadata: {db_err}")

    logger.info("Retention Management cycle complete.")

def main():
    logger.info("Starting Retention Manager daemon...")
    # Verify DB connection on startup
    try:
        conn = get_db_connection()
        conn.close()
    except Exception as e:
        logger.critical(f"Retention manager could not connect to PostgreSQL: {e}")
        time.sleep(5)

    # Run loop
    try:
        while True:
            try:
                run_retention_cycle()
            except Exception as e:
                logger.error(f"Error in retention cycle: {e}")
            
            # Sleep for 15 seconds for rapid local demo feedback loop
            time.sleep(15)
    except KeyboardInterrupt:
        logger.info("Retention Manager shutting down...")

if __name__ == "__main__":
    main()
