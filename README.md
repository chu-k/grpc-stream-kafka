1. Bring up kafka cluster and grpc server
```
docker compose up -d
```
2. Test python client publish (single, batch, and stream rpc)
```
cd data-client
python -m venv .venv && source .venv/bin/activate
pip install -e .
python examples/stream_msgs.py --num-messages 100 -max-size 7000 --num-threads 10
```

python client <-grpc-> grpc-service <-kafka(producer)-> cluster
