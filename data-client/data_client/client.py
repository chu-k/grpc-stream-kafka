from typing import List, Optional, Union
import grpc
from data_client.api import ingester_pb2
from data_client.api import ingester_pb2_grpc

class IngestionClient:
    """Client for interacting with the IngestionService."""

    def __init__(self, host: str = "localhost", port: int = 50051):
        """Initialize the client.

        Args:
            host: The host address of the gRPC server
            port: The port number of the gRPC server
        """
        self.channel = grpc.insecure_channel(f"{host}:{port}")
        self.stub = ingester_pb2_grpc.IngestionServiceStub(self.channel)

    def write(
        self,
        topic: str,
        payload: bytes,
        key: Optional[bytes] = None,
        client_id: Optional[str] = None
    ) -> bool:
        """Send a single message to Kafka.

        Args:
            topic: The Kafka topic to write to
            payload: The message payload as bytes
            key: Optional message key
            client_id: Optional client identifier

        Returns:
            bool: True if message was successfully delivered, False otherwise

        Raises:
            grpc.RpcError: If the RPC call fails
        """
        request = ingester_pb2.IngestionRequest(
            topic=topic,
            payload=payload,
            key=key if key is not None else b"",
            client_id=client_id if client_id is not None else ""
        )

        response = self.stub.IngestMessage(request)
        return response.success

    def write_batch(
        self,
        topic: str,
        entries: List[Union[bytes, tuple[bytes, Optional[bytes]]]],
        client_id: Optional[str] = None
    ) -> List[bool]:
        """Send a batch of messages to Kafka.

        Args:
            topic: The Kafka topic to write to
            entries: List of messages, where each message can be either:
                    - bytes (payload only)
                    - tuple[bytes, Optional[bytes]] (payload and optional key)
            client_id: Optional client identifier

        Returns:
            List[bool]: Success status for each message in the batch

        Raises:
            grpc.RpcError: If the RPC call fails
        """
        batch_entries = []
        for entry in entries:
            if isinstance(entry, bytes):
                payload, key = entry, None
            else:
                payload, key = entry

            batch_entry = ingester_pb2.BatchEntry(
                payload=payload,
                key=key if key is not None else b""
            )
            batch_entries.append(batch_entry)

        request = ingester_pb2.BatchIngestionRequest(
            topic=topic,
            entries=batch_entries,
            client_id=client_id if client_id is not None else ""
        )

        response = self.stub.IngestBatch(request)
        return list(response.success_statuses)

    def write_stream(
        self,
        topic: str,
        client_id: Optional[str] = None
    ):
        """Create a streaming context for sending messages to Kafka.

        Args:
            topic: The Kafka topic to write to
            client_id: Optional client identifier

        Returns:
            StreamingContext: A streaming context that can be used to send messages

        Example:
            with client.write_stream("my-topic") as stream:
                for message in messages:
                    success = stream.write(message)
        """
        return StreamingContext(self.stub, topic, client_id)

    def close(self):
        """Close the gRPC channel."""
        self.channel.close()


class StreamingContext:
    """Context manager for streaming messages to Kafka."""

    def __init__(self, stub, topic: str, client_id: Optional[str] = None):
        """Initialize the streaming context.

        Args:
            stub: The gRPC stub to use for the stream
            topic: The Kafka topic to write to
            client_id: Optional client identifier
        """
        self.stub = stub
        self.topic = topic
        self.client_id = client_id if client_id is not None else ""
        self.stream = None
        self.request_queue = []
        self._response_iter = None

    def __enter__(self):
        """Enter the context manager and start the stream."""
        def request_iterator():
            while True:
                if self.request_queue:
                    yield self.request_queue.pop(0)
                else:
                    import time
                    time.sleep(0.01)  # Small delay to prevent busy waiting

        self._response_iter = self.stub.WriteStream(request_iterator())
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the context manager and close the stream."""
        if self._response_iter:
            try:
                # Attempt to close the response iterator
                self._response_iter.cancel()
            except:
                pass
            self._response_iter = None

    def write(self, payload: bytes, key: Optional[bytes] = None) -> bool:
        """Write a message to the stream.

        Args:
            payload: The message payload as bytes
            key: Optional message key

        Returns:
            bool: True if message was successfully delivered, False otherwise

        Raises:
            grpc.RpcError: If the RPC call fails
        """
        if not self._response_iter:
            raise RuntimeError("Stream is not initialized. Use with statement.")

        request = ingester_pb2.IngestionRequest(
            topic=self.topic,
            payload=payload,
            key=key if key is not None else b"",
            client_id=self.client_id
        )

        self.request_queue.append(request)
        response = next(self._response_iter)
        return response.success
