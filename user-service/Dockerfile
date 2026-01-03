FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy proto files and compile
COPY protos/ ./protos/
RUN python -m grpc_tools.protoc \
    -I./protos \
    --python_out=. \
    --pyi_out=. \
    --grpc_python_out=. \
    ./protos/user.proto

# Copy application code
COPY main.py .

# Expose gRPC port
EXPOSE 50051

CMD ["python", "main.py"]
