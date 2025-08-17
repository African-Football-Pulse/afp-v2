# Dockerfile
FROM python:3.11-slim

# Installera systempaket (om något lib kräver t.ex. ssl, tzdata)
RUN apt-get update && apt-get install -y \
    tzdata \
 && rm -rf /var/lib/apt/lists/*

# Arbetskatalog
WORKDIR /app

# Kopiera requirements först (för att cacha pip-install mellan builds)
COPY requirements.txt .

# Installera Python-dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Kopiera resten av koden
COPY . /app  # Kopiera hela koden till /app

# Lista filer i /app/src/sections/ för felsökning
RUN ls /app/src/sections/

# Standardkommando kan override:as i ACA-jobben
CMD ["python", "-m", "src.collectors.rss_multi"]  # Default för collect-jobbet
