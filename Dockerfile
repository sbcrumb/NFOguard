FROM python:3.11-slim
WORKDIR /app

# deps (curl for healthcheck; gosu only needed when running as root)
RUN apt-get update && apt-get install -y --no-install-recommends \
    sqlite3 curl gosu \
  && rm -rf /var/lib/apt/lists/*

# Create a default appuser (will be ignored if process isn’t root)
RUN groupadd -g 1000 appuser && useradd -u 1000 -g appuser -m appuser

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY media_date_cache.py .
COPY webhook_server.py .

RUN mkdir -p /app/data && chown -R appuser:appuser /app

# Root-aware entrypoint
RUN cat > /entrypoint.sh << 'EOF'
#!/bin/bash
set -euo pipefail

PUID=${PUID:-1000}
PGID=${PGID:-1000}

if [ "$(id -u)" -eq 0 ]; then
  # running as root: we can adjust IDs and drop privileges
  groupmod -o -g "$PGID" appuser 2>/dev/null || groupadd -o -g "$PGID" appuser
  id appuser &>/dev/null || useradd -o -u "$PUID" -g "$PGID" -m appuser
  usermod -o -u "$PUID" -g "$PGID" appuser 2>/dev/null || true
  chown -R "$PUID:$PGID" /app || true
  exec gosu appuser "$@"
else
  # not root (e.g., compose set user: 1000:1000) — just run
  exec "$@"
fi
EOF
RUN chmod +x /entrypoint.sh

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:8080/health || exit 1

EXPOSE 8080
ENTRYPOINT ["/entrypoint.sh"]
CMD ["python", "webhook_server.py"]
