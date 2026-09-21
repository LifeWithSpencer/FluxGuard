FROM python:3.12-slim AS base

WORKDIR /app

# System deps kept minimal; gcc only needed transiently for a couple of
# wheels without manylinux binaries on some platforms.
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1

EXPOSE 8000

# Default command runs a single Uvicorn worker with uvloop; docker-compose
# overrides this for the gateway replicas to use Gunicorn+UvicornWorker
# for multi-process concurrency.
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--loop", "uvloop"]
