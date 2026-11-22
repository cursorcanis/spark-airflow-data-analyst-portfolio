# Small image for the GTFS-realtime poller (keeps the 3 GB Airflow image out of a 24/7 service).
FROM python:3.12.16-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/app/src
RUN pip install --no-cache-dir \
      "requests==2.32.*" "boto3==1.43.*" "gtfs-realtime-bindings==2.0.0" "protobuf>=6,<7" "pyarrow==25.*" \
 && useradd --create-home --uid 1000 poller

WORKDIR /app
COPY src/ /app/src/
USER poller

HEALTHCHECK --interval=60s --timeout=5s --start-period=60s \
  CMD python -c "import time,sys; sys.exit(0 if time.time()-int(open('/tmp/heartbeat').read())<180 else 1)"

CMD ["python", "-m", "atl_transit.rt_poller"]
