# syntax=docker/dockerfile:1
FROM python:3.11-slim

# Lägg till ffmpeg + Azure CLI
RUN apt-get update && apt-get install -y ffmpeg curl gnupg \
    && curl -sL https://aka.ms/InstallAzureCLIDeb | bash \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app \
    # valfri default – kan skrivas över i Job:
    JOB_TYPE=collect \
    SECRETS_FILE=/app/secrets/secret.json

WORKDIR /app

# Installera Python-paket
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install requests

# Källkod, config och jinglar
COPY src/ src/
COPY config/ config/
COPY assets/audio/ assets/audio/

# Entrypoint-skript för era vanliga flöden
COPY job_entrypoint.py /app/job_entrypoint.py

# Publiceringsskript för Buzzsprout (nytt)
COPY src/publisher/publish-to-buzzsprout.py /app/publish_to_buzzsprout.py

# Default kör fortfarande era vanliga jobb
ENTRYPOINT ["python","-u","/app/job_entrypoint.py"]
