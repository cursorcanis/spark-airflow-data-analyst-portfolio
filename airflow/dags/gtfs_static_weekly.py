"""Weekly ingest of the MARTA GTFS static feed into the raw zone of the data lake.

download -> validate -> skip if unchanged -> upload zip + extracted tables + manifest
Lake layout (S3-compatible bucket, RustFS locally / AWS S3 in the cloud):
    s3://raw/gtfs_static/ingest_date=YYYY-MM-DD/google_transit.zip
    s3://raw/gtfs_static/ingest_date=YYYY-MM-DD/tables/<file>.txt
    s3://raw/gtfs_static/ingest_date=YYYY-MM-DD/_manifest.json
    s3://raw/gtfs_static/_latest.json
"""

from __future__ import annotations

import json
import os
import shutil
import tempfile
import zipfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pendulum
import requests
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import dag, task
from lake_assets import RAW_GTFS_STATIC

from atl_transit.gtfs import validate_gtfs_zip

FEED_URL = os.environ.get("MARTA_GTFS_STATIC_URL", "https://www.itsmarta.com/google_transit_feed/google_transit.zip")
# MARTA's web server rejects the default python-requests User-Agent (HTTP 403), so identify the client.
USER_AGENT = "atl-transit-pipeline/0.1 (+https://github.com/cursorcanis/spark-airflow-data-analyst-portfolio)"
LAKE_CONN_ID = "lake_s3"
RAW_BUCKET = os.environ.get("LAKE_RAW_BUCKET", "raw")
PREFIX = "gtfs_static"
LATEST_KEY = f"{PREFIX}/_latest.json"


@dag(
    dag_id="gtfs_static_weekly",
    schedule="0 6 * * 1",  # Mondays 06:00 Atlanta time
    start_date=pendulum.datetime(2026, 11, 1, tz="America/New_York"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=10)},
    tags=["ingest", "gtfs", "marta"],
    doc_md=__doc__,
)
def gtfs_static_weekly():
    @task
    def download_and_validate() -> dict:
        workdir = Path(tempfile.mkdtemp(prefix="gtfs_static_"))
        zip_path = workdir / "google_transit.zip"
        with requests.get(FEED_URL, headers={"User-Agent": USER_AGENT}, stream=True, timeout=(10, 300)) as resp:
            resp.raise_for_status()
            with open(zip_path, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=1 << 20):
                    fh.write(chunk)

        summary = validate_gtfs_zip(zip_path)  # raises -> task fails -> retries
        return {
            **summary.to_dict(),
            "zip_path": str(zip_path),
            "source_url": FEED_URL,
            "downloaded_at": datetime.now(UTC).isoformat(timespec="seconds"),
        }

    @task.short_circuit(ignore_downstream_trigger_rules=False)
    def feed_changed(meta: dict) -> bool:
        hook = S3Hook(aws_conn_id=LAKE_CONN_ID)
        if not hook.check_for_bucket(RAW_BUCKET):
            hook.create_bucket(RAW_BUCKET)
            return True
        if not hook.check_for_key(LATEST_KEY, RAW_BUCKET):
            return True
        latest = json.loads(hook.read_key(LATEST_KEY, RAW_BUCKET))
        changed = latest.get("sha256") != meta["sha256"]
        if not changed:
            print(f"Feed unchanged (sha256 {meta['sha256'][:12]}); skipping upload.")
            shutil.rmtree(Path(meta["zip_path"]).parent, ignore_errors=True)
        return changed

    @task(outlets=[RAW_GTFS_STATIC])  # success -> triggers spark_curate_gtfs_static
    def upload(meta: dict) -> str:
        hook = S3Hook(aws_conn_id=LAKE_CONN_ID)
        zip_path = Path(meta["zip_path"])
        ingest_date = meta["downloaded_at"][:10]
        base = f"{PREFIX}/ingest_date={ingest_date}"
        try:
            hook.load_file(str(zip_path), f"{base}/google_transit.zip", RAW_BUCKET, replace=True)
            with zipfile.ZipFile(zip_path) as zf:
                for name in meta["row_counts"]:
                    hook.load_bytes(zf.read(name), f"{base}/tables/{name}", RAW_BUCKET, replace=True)

            manifest = {k: v for k, v in meta.items() if k != "zip_path"} | {"lake_prefix": f"s3://{RAW_BUCKET}/{base}/"}
            body = json.dumps(manifest, indent=2)
            hook.load_string(body, f"{base}/_manifest.json", RAW_BUCKET, replace=True)
            hook.load_string(body, LATEST_KEY, RAW_BUCKET, replace=True)
        finally:
            shutil.rmtree(zip_path.parent, ignore_errors=True)
        return manifest["lake_prefix"]

    meta = download_and_validate()
    feed_changed(meta) >> upload(meta)


gtfs_static_weekly()
