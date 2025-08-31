# syntax=docker/dockerfile:1
FROM python:3.11-slim

WORKDIR /app

# 1) Installera Python-paket
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 2) Kopiera in källkod och config in i imagen
COPY src/ src/
COPY config/ config/

# (behåll din entrypoint om du använder den)
ENV PYTHONPATH=/app
ENTRYPOINT ["python", "-m"]  # gör det enkelt att köra moduler
