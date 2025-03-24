#!/usr/bin/env python3
"""
gRPC Ingestion Server with Kafka integration
"""
import logging
import os
import time
import threading
from concurrent import futures
from typing import Dict, List, Optional

import grpc
from dotenv import load_dotenv
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable
from retrying import retry
from kafka.cluster import ClusterMetadata

from app import ingester_pb2
from app import ingester_pb2_grpc

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Server config defaults
DEFAULT_SERVER_ADDRESS = '0.0.0.0:50051'
DEFAULT_NUM_THREADS = 8
DEFAULT_MAX_MESSAGE_SIZE = 2147483647  # 2GB (2^31-1 bytes) - gRPC max message size limit

# Kafka config defaults
DEFAULT_KAFKA_BROKERS = 'kafka:9092'
DEFAULT_BATCH_SIZE = 10000
DEFAULT_LINGER_MS = 5
DEFAULT_RETRY_BACKOFF_MS = 100
DEFAULT_MESSAGE_TIMEOUT_MS = 30000


class KafkaManager:
    """Manages Kafka producers and message delivery"""

    def __init__(self, config: Dict):
        """Initialize with Kafka configuration"""
        self.config = config
        self.producers: Dict[str, KafkaProducer] = {}
        self.producers_lock = threading.Lock()
        logger.info(f"KafkaManager initialized with brokers: {config['kafka_brokers']}")

    @retry(
        stop_max_attempt_number=10,
        wait_exponential_multiplier=1000,  # Initial wait 1s (in ms)
        wait_exponential_max=30000,        # Max wait 30s (in ms)
        retry_on_exception=lambda e: isinstance(e, (KafkaError, NoBrokersAvailable))
    )
    def probe_connection(self) -> bool:
        """Probe connection to Kafka cluster with retries"""
        try:
            # Create cluster metadata instance
            cluster = ClusterMetadata(bootstrap_servers=self.config['kafka_brokers'])

            # Check if we can get broker information
            brokers = cluster.brokers()
            if not brokers:
                raise KafkaError("No brokers available in cluster")

            # Try to list topics as additional connection verification
            topics = cluster.topics()

            logger.info(f"Successfully connected to Kafka cluster. Found {len(brokers)} brokers")
            logger.debug(f"Available topics: {topics}")
            return True

        except (KafkaError, NoBrokersAvailable) as e:
            logger.warning(f"Failed to connect to Kafka cluster: {str(e)}, retrying...")
            raise

    def get_producer(self, topic: str) -> KafkaProducer:
        """Get or create a producer for the specified topic"""
        with self.producers_lock:
            if topic not in self.producers:
                producer = KafkaProducer(
                    bootstrap_servers=self.config['kafka_brokers'],
                    batch_size=self.config['batch_size'],
                    linger_ms=self.config['linger_ms'],
                    retry_backoff_ms=self.config['retry_backoff_ms'],
                    request_timeout_ms=self.config['message_timeout_ms'],
                    compression_type=self.config['compression_type'],
                    acks=self.config['acks']
                )
                self.producers[topic] = producer
                logger.info(f"Created new producer for topic: {topic}")

            return self.producers[topic]

    def send_message(self, topic: str, key: Optional[bytes], payload: bytes) -> bool:
        """Send a single message to Kafka"""
        try:
            start_time = time.time()
            message_size = len(payload) + (len(key) if key else 0)

            producer = self.get_producer(topic)
            future = producer.send(
                topic=topic,
                key=key,
                value=payload
            )

            # Wait for the message to be delivered
            record_metadata = future.get(timeout=10)
            elapsed_ms = (time.time() - start_time) * 1000
            throughput = message_size / elapsed_ms  # bytes/ms

            logger.info(f"Message sent to {topic} [{record_metadata.partition}] at offset {record_metadata.offset}. "
                       f"Throughput: {throughput:.2f} bytes/ms ({message_size} bytes in {elapsed_ms:.2f}ms)")
            return True

        except KafkaError as e:
            logger.error(f"Failed to send message to {topic}: {str(e)}")
            return False

    def send_batch(self, topic: str, keys: List[Optional[bytes]], payloads: List[bytes]) -> List[bool]:
        """Send a batch of messages to Kafka"""
        if len(keys) != len(payloads):
            return [False] * len(payloads)

        start_time = time.time()
        total_size = sum(len(p) + (len(k) if k else 0) for k, p in zip(keys, payloads))

        producer = self.get_producer(topic)
        results = []

        for i, (key, payload) in enumerate(zip(keys, payloads)):
            try:
                future = producer.send(
                    topic=topic,
                    key=key,
                    value=payload
                )
                results.append(True)

                # Occasionally poll to handle callbacks
                if i % 1000 == 0:
                    producer.flush(timeout=0)

            except KafkaError as e:
                logger.error(f"Failed to produce message {i} to {topic}: {str(e)}")
                results.append(False)

        # Final flush to ensure delivery
        producer.flush(timeout=1.0)

        elapsed_ms = (time.time() - start_time) * 1000
        throughput = total_size / elapsed_ms  # bytes/ms
        success_rate = sum(results) / len(results) * 100

        logger.info(f"Batch sent to {topic}. Success rate: {success_rate:.1f}%. "
                    f"Throughput: {throughput:.2f} bytes/ms ({total_size} bytes in {elapsed_ms:.2f}ms)")

        return results

    def close(self):
        """Close all Kafka producers"""
        with self.producers_lock:
            for topic, producer in self.producers.items():
                producer.close()
            self.producers.clear()


class IngestionServiceImpl(ingester_pb2_grpc.IngestionServiceServicer):
    """gRPC service implementation for ingestion requests"""

    def __init__(self, kafka_manager: KafkaManager):
        self.kafka_manager = kafka_manager

    def IngestMessage(self, request, context):
        """Handle single message ingestion"""
        topic = request.topic
        payload = request.payload
        key = request.key if request.key else None
        client_id = request.client_id

        logger.info(f"IngestMessage request from {client_id} for topic {topic}")

        success = self.kafka_manager.send_message(topic, key, payload)

        response = ingester_pb2.IngestionResponse(
            success=success,
            error_message="" if success else "Failed to deliver message to Kafka"
        )
        return response

    def IngestBatch(self, request, context):
        """Handle batch message ingestion"""
        topic = request.topic
        client_id = request.client_id

        # Extract entries
        keys = []
        payloads = []

        for entry in request.entries:
            keys.append(entry.key if entry.key else None)
            payloads.append(entry.payload)

        batch_size = len(payloads)
        logger.info(f"IngestBatch request from {client_id} for topic {topic} with {batch_size} messages")

        # Send batch to Kafka
        success_statuses = self.kafka_manager.send_batch(topic, keys, payloads)

        # Create response
        response = ingester_pb2.BatchIngestionResponse(
            success_statuses=success_statuses,
            error_message="" if all(success_statuses) else "Some messages failed to be delivered"
        )
        return response

    def WriteStream(self, request_iterator, context):
        """Handle streaming message ingestion"""
        for request in request_iterator:
            topic = request.topic
            payload = request.payload
            key = request.key if request.key else None
            client_id = request.client_id

            logger.info(f"WriteStream message from {client_id} for topic {topic}")

            success = self.kafka_manager.send_message(topic, key, payload)

            response = ingester_pb2.IngestionResponse(
                success=success,
                error_message="" if success else "Failed to deliver message to Kafka"
            )
            yield response

    def TopicStream(self, request, context):
        """Handle topic stream requests"""
        topic = request.topic
        client_id = request.client_id

        logger.info(f"TopicStream request from {client_id} for topic {topic}")

        try:
            # Create a Kafka consumer for the topic
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=self.kafka_manager.config['kafka_brokers'],
                group_id=f"topic_stream_{client_id}",  # Unique consumer group per client
                auto_offset_reset='latest',
                enable_auto_commit=True,
                consumer_timeout_ms=1000  # 1 second timeout for poll
            )

            # Stream messages to the client
            try:
                while context.is_active():  # Check if client is still connected
                    # Poll for messages
                    message_batch = consumer.poll(timeout_ms=1000)

                    for partition_batch in message_batch.values():
                        for message in partition_batch:
                            # Create response message
                            response = ingester_pb2.TopicStreamResponse(
                                success=True,
                                payload=message.value,
                                key=message.key if message.key else None,
                                partition=message.partition,
                                offset=message.offset,
                                timestamp=message.timestamp
                            )
                            yield response

            except Exception as e:
                logger.error(f"Error while streaming messages for {client_id}: {str(e)}")
                yield ingester_pb2.TopicStreamResponse(
                    success=False,
                    error_message=f"Stream error: {str(e)}"
                )
            finally:
                # Clean up consumer
                consumer.close()
                logger.info(f"Closed Kafka consumer for client {client_id}")

        except Exception as e:
            logger.error(f"Failed to create consumer for {client_id}: {str(e)}")
            yield ingester_pb2.TopicStreamResponse(
                success=False,
                error_message=f"Failed to create consumer: {str(e)}"
            )


def serve():
    """Start the server"""
    # Load configuration
    server_config = {
        'server_address': os.getenv('SERVER_ADDRESS', DEFAULT_SERVER_ADDRESS),
        'num_threads': int(os.getenv('NUM_THREADS', DEFAULT_NUM_THREADS)),
        'max_message_size': int(os.getenv('MAX_MESSAGE_SIZE', DEFAULT_MAX_MESSAGE_SIZE)),
        'kafka_brokers': os.getenv('KAFKA_BROKERS', DEFAULT_KAFKA_BROKERS),
        'batch_size': int(os.getenv('BATCH_SIZE', DEFAULT_BATCH_SIZE)),
        'linger_ms': int(os.getenv('LINGER_MS', DEFAULT_LINGER_MS)),
        'retry_backoff_ms': int(os.getenv('RETRY_BACKOFF_MS', DEFAULT_RETRY_BACKOFF_MS)),
        'message_timeout_ms': int(os.getenv('MESSAGE_TIMEOUT_MS', DEFAULT_MESSAGE_TIMEOUT_MS)),
        'compression_type': os.getenv('COMPRESSION_TYPE', 'snappy'),
        'acks': os.getenv('ACKS', 'all'),
    }

    # Create Kafka manager and probe connection
    kafka_manager = KafkaManager(server_config)
    try:
        kafka_manager.probe_connection()
    except Exception as e:
        logger.error(f"Failed to establish connection to Kafka after retries: {str(e)}")
        return  # Exit if we can't connect to Kafka

    # Create gRPC server
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=server_config['num_threads']),
        options=[
            ('grpc.max_receive_message_length', server_config['max_message_size']),
        ]
    )

    # Add service to server
    ingester_pb2_grpc.add_IngestionServiceServicer_to_server(
        IngestionServiceImpl(kafka_manager),
        server
    )

    # Start server
    server.add_insecure_port(server_config['server_address'])
    server.start()

    logger.info(f"Server started on {server_config['server_address']}")
    logger.info(f"Using Kafka brokers: {server_config['kafka_brokers']}")

    try:
        while True:
            time.sleep(60*60*24)  # Sleep for 1 day
    except KeyboardInterrupt:
        logger.info("Shutting down server...")
        server.stop(grace=None)
        kafka_manager.close()


if __name__ == '__main__':
    serve()
