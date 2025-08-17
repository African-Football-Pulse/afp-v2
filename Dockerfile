FROM python:3.11-slim
WORKDIR /app
COPY . /app
RUN pip install --no-cache-dir azure-storage-blob azure-identity pyyaml jinja2
ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python"]
