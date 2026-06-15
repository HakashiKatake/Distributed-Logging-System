import json
import logging
import queue
import time
from source.config import settings

logger = logging.getLogger("dls.kafka")

class KafkaClient:
    def __init__(self):
        self.producer = None
        self._in_memory_queue = queue.Queue() # Resilient fallback
        self._is_fallback = False
        self._connect_producer()

    def _connect_producer(self):
        # Retry connection for startup sync
        retries = 3
        while retries > 0:
            try:
                # Import here to avoid forcing confluent-kafka installation errors if testing isolated files
                from kafka import KafkaProducer
                
                # In docker compose, kafka advertised port is 9092. On host, it is 9094.
                # Let's try both configurations.
                servers = [settings.KAFKA_BOOTSTRAP_SERVERS, "localhost:9094", "localhost:9092", "kafka:9092"]
                for server in servers:
                    try:
                        self.producer = KafkaProducer(
                            bootstrap_servers=server,
                            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                            request_timeout_ms=3000,
                            api_version=(2, 5, 0)
                        )
                        logger.info(f"Connected Kafka Producer successfully to {server}")
                        self._is_fallback = False
                        return
                    except Exception:
                        continue
            except ImportError:
                logger.warning("kafka-python-ng not installed or import error. Falling back to in-memory queue.")
                break
            except Exception as e:
                logger.warning(f"Failed Kafka Producer connection attempt. Retrying... Error: {e}")
            
            retries -= 1
            time.sleep(2)
        
        logger.warning("Kafka Broker unreachable. Falling back to in-memory resilient log buffer.")
        self._is_fallback = True

    def publish_log(self, log_entry: dict) -> bool:
       
        if self._is_fallback or not self.producer:
            # Enqueue to local memory queue
            self._in_memory_queue.put(log_entry)
            return True
        try:
            future = self.producer.send(settings.KAFKA_TOPIC, value=log_entry)
            # Block briefly to guarantee durability during ingestion
            future.get(timeout=3)
            return True
        except Exception as e:
            logger.error(f"Failed to publish to Kafka: {e}. Appending to in-memory fallback queue.")
            self._in_memory_queue.put(log_entry)
            return True

    def get_fallback_queue_size(self) -> int:
        return self._in_memory_queue.qsize()

    def get_consumer(self, group_id: str):
       
        if self._is_fallback:
            return MockConsumer(self._in_memory_queue)
        
        try:
            from kafka import KafkaConsumer
            servers = [settings.KAFKA_BOOTSTRAP_SERVERS, "localhost:9094", "localhost:9092", "kafka:9092"]
            for server in servers:
                try:
                    consumer = KafkaConsumer(
                        settings.KAFKA_TOPIC,
                        bootstrap_servers=server,
                        group_id=group_id,
                        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                        auto_offset_reset='earliest',
                        enable_auto_commit=True,
                        api_version=(2, 5, 0)
                    )
                    logger.info(f"Connected Kafka Consumer to {server} with group {group_id}")
                    return consumer
                except Exception:
                    continue
        except Exception as e:
            logger.error(f"Could not initialize Kafka Consumer: {e}")
            
        logger.warning("Using mock consumer fallback.")
        return MockConsumer(self._in_memory_queue)

class MockConsumer:
    
    def __init__(self, q: queue.Queue):
        self._queue = q

    def __iter__(self):
        return self

    def __next__(self):
        # Block until a log is available
        try:
            # Use small timeout to allow graceful exits
            item = self._queue.get(timeout=1.0)
            # Wrap to match Kafka consumer record format (needs value property)
            return MockRecord(item)
        except queue.Empty:
            raise StopIteration

    def poll(self, timeout_ms=1000):
        # Emulate polling
        records = []
        start_time = time.time()
        while (time.time() - start_time) < (timeout_ms / 1000.0):
            try:
                item = self._queue.get_nowait()
                records.append(MockRecord(item))
            except queue.Empty:
                break
        return {settings.KAFKA_TOPIC: records} if records else {}

class MockRecord:
    def __init__(self, value):
        self.value = value
        self.topic = settings.KAFKA_TOPIC
        self.partition = 0
        self.offset = 0

# Singleton instance
kafka_client = KafkaClient()
