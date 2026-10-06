FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# SQLite database lives in /data (mounted as a volume for persistence).
ENV DATABASE_PATH=/data/orchestrator.db \
    DEFAULT_LEASE_SECONDS=30 \
    MAX_LEASE_SECONDS=3600

RUN useradd --create-home appuser && mkdir -p /data && chown -R appuser /data /app
USER appuser

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/healthz')"

# Schema initialization happens automatically at startup (CREATE TABLE IF NOT EXISTS).
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
