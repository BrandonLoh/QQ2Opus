FROM python:3.11-slim

# ── System dependencies ────────────────────────────────────────────
RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# ── App user (PUID/PGID override at runtime) ───────────────────────
ARG PUID=1000
ARG PGID=1000
RUN groupmod -g $PGID www-data 2>/dev/null || true \
    && usermod -u $PUID -g $PGID www-data 2>/dev/null || true

# ── App directory ──────────────────────────────────────────────────
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ ./app/

# ── Cache volume mount point ───────────────────────────────────────
RUN mkdir -p /cache \
    && chown -R www-data:www-data /cache /app

USER www-data

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--log-level", "warning"]
