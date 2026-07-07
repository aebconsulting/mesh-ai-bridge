FROM python:3.12-slim
LABEL org.opencontainers.image.source=https://github.com/aebconsulting/mesh-ai-bridge
LABEL org.opencontainers.image.description="Meshtastic mesh -> local LLM bridge (NOMAD custom app; radio via ser2net TCP)"
RUN pip install --no-cache-dir meshtastic==2.7.10 requests
# Run as the native services identity (ai_box:dialout) so the volume-mounted
# memory.db stays writable by both the container and the native rollback path.
USER 1000:20
ENV PYTHONUNBUFFERED=1
COPY bridge.py /app/bridge.py
CMD ["python", "/app/bridge.py"]
