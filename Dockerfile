# Dockerfile
FROM python:3.11-slim

# Installera systempaket (om något lib kräver t.ex. ssl, tzdata)
RUN apt-get update && apt-get install -y \
    tzdata \
 && rm -rf /var/lib/apt/lists/*

# Sätt arbetskatalogen (skapas automatiskt om den inte finns)
WORKDIR /app

# Kopiera requirements först (för att cacha pip-install mellan builds)
COPY requirements.txt /app/

# Installera Python-dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Kopiera resten av koden och projektfiler
COPY . /app/

# Lista filer i /app/src/sections/ för felsökning (valfritt felsökningssteg)
RUN ls /app/src/sections/ || true

# Standardkommando, kan override:as i ACA-jobben
CMD ["python", "-m", "src.collectors.rss_multi"]
