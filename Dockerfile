# syntax=docker/dockerfile:1
FROM python:3.11-slim

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

# Secrets (läggs in i imagen under /app/secrets/)
COPY secrets/ /app/secrets/

# Entrypoint-skript som startar rätt modul och laddar hemligheter
COPY job_entrypoint.py /app/job_entrypoint.py

# Kör alltid via entrypoint-skriptet
ENTRYPOINT ["python","-u","/app/job_entrypoint.py"]
