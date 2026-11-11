FROM python:3.10-slim

WORKDIR /app

# Install Java runtime (required for PySpark), GCC, libpq, and process utilities
RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jre-headless gcc libpq-dev procps curl \
    && rm -rf /var/lib/apt/lists/*

# Security hardening: Dedicated non-root container user
RUN useradd -m -u 10001 appuser

COPY requirements.txt .
RUN pip install --default-timeout=1000 --retries=10 --no-cache-dir -r requirements.txt

COPY stream_process.py .
RUN chown -R appuser:appuser /app

USER appuser

CMD ["python", "stream_process.py"]
