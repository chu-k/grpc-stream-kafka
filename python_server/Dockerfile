FROM python:3.11-slim

WORKDIR /app

# Install system dependencies for python-snappy
RUN apt-get update && apt-get install -y \
    python3-dev \
    gcc \
    libsnappy-dev \
    && rm -rf /var/lib/apt/lists/*

# Install dependencies directly
RUN pip install grpcio==1.59.0 grpcio-tools==1.59.0 protobuf==4.24.4 kafka-python==2.0.2 python-dotenv==1.0.0 python-snappy retrying

# Create app directory
RUN mkdir -p app

# The protos directory will be mounted as a volume

# Create entrypoint script
RUN echo '#!/bin/bash\n\
# Generate protobuf files\n\
python -m grpc_tools.protoc -I/app/protos --python_out=/app/app --grpc_python_out=/app/app /app/protos/ingester.proto\n\
# Fix imports in generated files\n\
sed -i "s/import ingester_pb2/import app.ingester_pb2/" /app/app/ingester_pb2_grpc.py\n\
# Start the server\n\
exec python -u /app/app/server.py\n\
' > /app/entrypoint.sh && chmod +x /app/entrypoint.sh

# Copy server implementation
COPY python_server/app/server.py /app/app/

# Set environment variables
ENV PYTHONPATH=/app

EXPOSE 50051

ENTRYPOINT ["/app/entrypoint.sh"]
