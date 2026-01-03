FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy proto files and compile
COPY protos/ ./protos/
RUN mkdir -p /app/generated && \
    python -m grpc_tools.protoc \
    -I./protos \
    --python_out=/app/generated \
    --pyi_out=/app/generated \
    --grpc_python_out=/app/generated \
    ./protos/user.proto

COPY main.py .

# Create static directory for web client
RUN mkdir -p /app/static

EXPOSE 8000

CMD ["python", "main.py"]
