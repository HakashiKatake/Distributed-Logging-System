import time
import logging
from opensearchpy import OpenSearch, exceptions
from source.config import settings

logger = logging.getLogger("dls.opensearch")

class OpenSearchClient:
    def __init__(self):
        self.client = None
        self._connect()

    def _connect(self):
        host = settings.OPENSEARCH_HOST
        port = settings.OPENSEARCH_PORT
        auth = (settings.OPENSEARCH_USER, settings.OPENSEARCH_PASSWORD)

        # Retry logic for local setup
        retries = 5
        while retries > 0:
            try:
                # DISABLE_SECURITY_PLUGIN=true is configured in docker-compose for simplicity.
                # So we can connect without SSL/auth or with it.
                self.client = OpenSearch(
                    hosts=[{'host': host, 'port': port}],
                    http_auth=auth,
                    use_ssl=False,
                    verify_certs=False,
                    ssl_assert_hostname=False,
                    ssl_show_warn=False
                )
                # Test connection
                self.client.info()
                logger.info(f"Connected to OpenSearch at {host}:{port}")
                self._create_index_template()
                return
            except Exception as e:
                logger.warning(f"Failed to connect to OpenSearch. Retrying in 2s... Error: {e}")
                retries -= 1
                time.sleep(2)
        logger.error("Could not connect to OpenSearch. Running in fallback/mock mode.")
        self.client = None

    def _create_index_template(self):
        """Creates mapping template for logs-* indices to ensure correct types."""
        if not self.client:
            return
        
        template_name = "dls-logs-template"
        template_body = {
            "index_patterns": ["logs-*"],
            "template": {
                "mappings": {
                    "properties": {
                        "@timestamp": {"type": "date"},
                        "service_name": {"type": "keyword"},
                        "host_name": {"type": "keyword"},
                        "level": {"type": "keyword"},
                        "request_id": {"type": "keyword"},
                        "trace_id": {"type": "keyword"},
                        "message": {"type": "text"},
                        "source": {"type": "keyword"},
                        "payload": {"type": "object"}
                    }
                }
            }
        }
        try:
            self.client.indices.put_template(name=template_name, body=template_body)
            logger.info("OpenSearch log index template created/updated.")
        except Exception as e:
            logger.error(f"Error creating index template: {e}")

    def index_log(self, index_name: str, log_doc: dict):
        """Indexes a log document into the specified index."""
        if not self.client:
            logger.warning(f"OpenSearch unavailable. Mock-indexing: {log_doc}")
            return True
        try:
            self.client.index(index=index_name, body=log_doc)
            return True
        except Exception as e:
            logger.error(f"Failed to index log in OpenSearch: {e}")
            return False

    def search_logs(self, service_name=None, level=None, start_time=None, end_time=None, request_id=None, query_string=None, limit=100):
        """Performs structured and full-text keyword searches on logs-* indices."""
        if not self.client:
            logger.warning("OpenSearch client is not connected. Returning empty query results.")
            return []

        # Build ES query DSL
        must_clauses = []

        if service_name:
            must_clauses.append({"match": {"service_name": {"query": service_name, "operator": "and"}}})
        if level:
            must_clauses.append({"match": {"level": {"query": level, "operator": "and"}}})
        if request_id:
            must_clauses.append({"match": {"request_id": {"query": request_id, "operator": "and"}}})
        
        # Time range query
        if start_time or end_time:
            time_range = {}
            if start_time:
                time_range["gte"] = start_time
            if end_time:
                time_range["lte"] = end_time
            must_clauses.append({"range": {"@timestamp": time_range}})

        # Full-text search on message
        if query_string:
            must_clauses.append({"query_string": {"query": query_string, "default_field": "message"}})

        query_body = {
            "size": limit,
            "sort": [{"@timestamp": {"order": "desc"}}],
            "query": {
                "bool": {
                    "must": must_clauses if must_clauses else {"match_all": {}}
                }
            }
        }

        try:
            res = self.client.search(index="logs-*", body=query_body)
            hits = res.get("hits", {}).get("hits", [])
            return [hit["_source"] for hit in hits]
        except exceptions.NotFoundError:
            # Index does not exist yet (no logs ingested)
            return []
        except Exception as e:
            logger.error(f"OpenSearch search query failed: {e}")
            return []

    def get_logs_before(self, timestamp_str: str):
        """Fetches all logs older than a timestamp to prepare for archiving."""
        if not self.client:
            return []
        
        query_body = {
            "size": 5000,
            "query": {
                "range": {
                    "@timestamp": {
                        "lt": timestamp_str
                    }
                }
            }
        }
        try:
            res = self.client.search(index="logs-*", body=query_body)
            hits = res.get("hits", {}).get("hits", [])
            return [hit["_source"] for hit in hits]
        except exceptions.NotFoundError:
            return []
        except Exception as e:
            logger.error(f"Failed to fetch old logs for archive: {e}")
            return []

    def delete_logs_before(self, timestamp_str: str):
        """Deletes logs older than a timestamp from hot storage."""
        if not self.client:
            return 0
        
        query_body = {
            "query": {
                "range": {
                    "@timestamp": {
                        "lt": timestamp_str
                    }
                }
            }
        }
        try:
            res = self.client.delete_by_query(index="logs-*", body=query_body)
            deleted = res.get("deleted", 0)
            logger.info(f"Purged {deleted} expired logs from OpenSearch indices.")
            return deleted
        except exceptions.NotFoundError:
            return 0
        except Exception as e:
            logger.error(f"Failed to delete old logs from OpenSearch: {e}")
            return 0

# Singleton instance
opensearch_client = OpenSearchClient()
