FROM python:3.10-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jre-headless gcc libpq-dev procps \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -m -u 10001 appuser

COPY requirements.txt .
RUN pip install --default-timeout=1000 --retries=10 --no-cache-dir -r requirements.txt

COPY spark_process.py .
RUN chown -R appuser:appuser /app

USER appuser

CMD ["python", "spark_process.py"]
