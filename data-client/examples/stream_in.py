import asyncio
import grpc
import signal
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Optional
from data_client.api import ingester_pb2
from data_client.api import ingester_pb2_grpc

class KafkaStreamClient:
    def __init__(
        self,
        server_address: str,
        client_id: str,
        message_handler: Optional[Callable] = None
    ):
        self.server_address = server_address
        self.client_id = client_id
        self.message_handler = message_handler or self._default_handler
        self.running = False
        self._channel = None
        self._stub = None

    async def connect(self):
        """Establish connection to the gRPC server"""
        self._channel = grpc.aio.insecure_channel(self.server_address)
        self._stub = ingester_pb2_grpc.IngestionServiceStub(self._channel)

    async def close(self):
        """Close the connection"""
        if self._channel:
            await self._channel.close()

    def _default_handler(self, response: ingester_pb2.TopicStreamResponse):
        """Default message handler"""
        if response.success:
            print(f"Message received from partition {response.partition}, "
                  f"offset {response.offset}:")
            print(f"  Payload: {response.payload}")
            print(f"  Key: {response.key}")
            print(f"  Timestamp: {response.timestamp}")
        else:
            print(f"Error: {response.error_message}")

    async def stream_topic(self, topic: str):
        """
        Stream messages from a Kafka topic

        Args:
            topic: The Kafka topic to stream from
        """
        self.running = True
        request = ingester_pb2.TopicStreamRequest(
            topic=topic,
            client_id=self.client_id
        )

        while self.running:
            try:
                async for response in self._stub.TopicStream(request):
                    if not self.running:
                        break

                    # Process message in thread pool to avoid blocking
                    loop = asyncio.get_event_loop()
                    with ThreadPoolExecutor() as pool:
                        await loop.run_in_executor(
                            pool,
                            self.message_handler,
                            response
                        )

            except grpc.aio.AioRpcError as e:
                print(f"Stream error: {e}")
                if e.code() == grpc.StatusCode.UNAVAILABLE:
                    print("Server unavailable, retrying in 5 seconds...")
                    await asyncio.sleep(5)
                else:
                    self.running = False
                    raise

async def main():
    # Example custom message handler
    def custom_handler(response):
        if response.success:
            # Do something with the message
            print(f"Processing message from offset {response.offset}")
            # ... your processing logic here ...

    # Create the client
    client = KafkaStreamClient(
        server_address="localhost:50051",
        client_id="client-001",
        message_handler=custom_handler
    )

    # Handle graceful shutdown
    def signal_handler():
        print("Shutting down...")
        client.running = False

    # Register signal handlers
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, signal_handler)

    try:
        # Connect and start streaming
        await client.connect()
        await client.stream_topic("my-topic")
    finally:
        await client.close()

if __name__ == "__main__":
    asyncio.run(main())
