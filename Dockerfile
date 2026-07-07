FROM python:3.12-slim
LABEL org.opencontainers.image.source=https://github.com/aebconsulting/mesh-ai-bridge
LABEL org.opencontainers.image.description="Meshtastic mesh -> local LLM bridge (NOMAD custom app; radio via ser2net TCP)"
RUN pip install --no-cache-dir meshtastic==2.7.10 requests
# The bridge runs as a non-root UID. Pre-create and own the default DB directory and the
# conventional /data mount point so the SQLite memory DB is writable — a fresh Docker named
# volume mounted at /data inherits this ownership, so the quick-start works out of the box.
RUN mkdir -p /opt/mesh-ai-bridge /data && chown 1000:20 /opt/mesh-ai-bridge /data
# Run as a fixed non-root UID:GID (keeps a bind-mounted DB writable by the host user too).
USER 1000:20
ENV PYTHONUNBUFFERED=1
COPY bridge.py /app/bridge.py
CMD ["python", "/app/bridge.py"]
