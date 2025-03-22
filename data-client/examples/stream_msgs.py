from concurrent.futures import ThreadPoolExecutor
from data_client.client import IngestionClient
import argparse
import random
import string

def generate_random_payload(max_size):
    # Generate a random size between 1/2 max_size and max_size
    size = random.randint(max_size // 2, max_size)
    # Generate random string of specified size
    return ''.join(random.choices(string.ascii_letters + string.digits, k=size)).encode()

def publish_messages(client, topic, messages, thread_id):
    """
    Publish messages using individual, batch, and streaming methods.

    Args:
        client: IngestionClient instance
        messages: List of (payload, key) tuples to publish
        thread_id: Identifier for the current thread
    """
    try:
        # Send all as individual messages
        for i, (payload, key) in enumerate(messages, 1):
            success = client.write(topic, payload, key, f"thread-{thread_id}")
            print(f"Thread {thread_id}: Individual publish {i}/{len(messages)} - Success: {success}")

        # Send all as a batch
        results = client.write_batch(topic, messages, f"thread-{thread_id}")
        successful = sum(results)
        print(f"Thread {thread_id}: Batch publish - {successful}/{len(messages)} messages successful")

        # Send all using streaming context
        with client.write_stream(topic, f"thread-{thread_id}") as stream:
            for payload, key in messages:  # Unpack the tuple
                success = stream.write(payload, key)
                if not success:
                    print("Failed to send message")
        print(f"Thread {thread_id}: Streaming publish - {successful}/{len(messages)} messages successful")

    except Exception as e:
        print(f"Thread {thread_id}: Error publishing messages: {e}")
        raise

def main():
    # Set up command line arguments
    parser = argparse.ArgumentParser(description='Kafka message ingestion example')
    parser.add_argument('--num-messages', '-n', type=int, default=50,
                      help='Number of messages to send per thread')
    parser.add_argument('--max-size', '-s', type=int, default=1000,
                      help='Maximum size of each message in bytes')
    parser.add_argument('--num-threads', '-t', type=int, default=3,
                      help='Number of concurrent publishing threads')
    args = parser.parse_args()

    # Create a client instance
    client = IngestionClient(host="localhost", port=50051)

    try:
        # Generate messages for each thread (each thread gets its own full set)
        thread_message_chunks = []
        for thread_id in range(args.num_threads):
            messages = [
                (generate_random_payload(args.max_size), f"thread{thread_id}-msg{i}".encode())
                for i in range(args.num_messages)
            ]
            thread_message_chunks.append(messages)

        # Use ThreadPoolExecutor for concurrent publishing
        with ThreadPoolExecutor(max_workers=args.num_threads) as executor:
            futures = []
            for thread_id, msg_chunk in enumerate(thread_message_chunks):
                future = executor.submit(publish_messages, client, f"topic-{thread_id}", msg_chunk, thread_id + 1)
                futures.append(future)

            # Wait for all threads to complete
            for future in futures:
                future.result()

    finally:
        # Always close the client when done
        client.close()

if __name__ == "__main__":
    main()
