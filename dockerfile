FROM python:3.11-slim
WORKDIR /app
COPY . /app
RUN pip install --no-cache-dir azure-storage-blob pyyaml jinja2
ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python"]
# vi kommer överstyra kommandot per jobb, t.ex. -m src.collectors.collect_data
