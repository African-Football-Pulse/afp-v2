# syntax=docker/dockerfile:1
FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source and config
COPY src/ src/
COPY config/ config/
COPY job_entrypoint.py .

ENV PYTHONPATH=/app

# Default entrypoint
ENTRYPOINT ["python", "job_entrypoint.py"]
