# syntax=docker/dockerfile:1
FROM python:3.11-slim

# Lägg till ffmpeg
RUN apt-get update && apt-get install -y ffmpeg && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app \
    # valfri default – kan skrivas över i Job:
    JOB_TYPE=collect \
    SECRETS_FILE=/app/secrets/secret.json

WORKDIR /app

# Installera Python-paket
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Källkod och config
COPY src/ src/
COPY config/ config/

# Entrypoint-skript som startar rätt modul och laddar hemligheter
COPY job_entrypoint.py /app/job_entrypoint.py

# Kör alltid via entrypoint-skriptet (ingen portal-override behövs)
ENTRYPOINT ["python","-u","/app/job_entrypoint.py"]
