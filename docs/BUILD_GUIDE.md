# Build Guide: how the platform is put together, step by step

This guide rebuilds the whole platform by hand, in order. Each step says **what** you do, **why** it is done that way, and **how it connects** to the pieces before and after it. Follow it top to bottom on a clean machine and you end up with exactly what runs in this repo.

Placeholders: `<SERVER>` = the server's hostname or IP (e.g. its Tailscale IP), `<USER>` = your Linux user on the server.

---

## Step 0: The big picture (read this first)

The platform is a chain. Each link hands data to the next one:

```
Public website ──► Airflow ──► Data lake (S3 API) ──► Spark ──► Postgres/PostGIS (dbt) ──► Power BI / map
 (MARTA GTFS)     (scheduler)   (raw files)           (cleaning)  (modeled tables)          (people)
```

| Piece | Job in one sentence | Why this tool |
|---|---|---|
| **Docker + Compose** | Runs every service in its own isolated container from one config file | One command rebuilds the whole platform on any machine |
| **Airflow** | Decides *when* each step runs, in *what order*, and retries failures | Industry-standard orchestrator; pipelines are Python code |
| **Data lake (RustFS)** | Stores raw and curated files cheaply, organized by folders ("prefixes") | Speaks the Amazon S3 API, so the same code works on AWS |
| **Spark** | Cleans and reshapes large data in parallel | Scales from a laptop to a cluster without code changes |
| **Postgres + PostGIS** | The warehouse: clean, modeled tables that BI tools query | Free, SQL-standard, best-in-class spatial functions |
| **dbt** | Turns raw tables into tested, documented models using SQL | Version-controlled SQL with built-in data tests |
| **GitHub + Actions** | Holds the code and runs tests on every push | Proves the code works; the deploy source for the server |

**Two machines, one direction of flow for code:**

```
Dev PC (edit code) ──git push──► GitHub ──git pull──► Server (runs Docker)
```

The server never gets hand-edited code. Data (lake files, database rows, logs) lives only on the server, in Docker volumes.

---

## Step 1: Prepare the server

**Do:**
1. Install Ubuntu (24.04+). Update it: `sudo apt update && sudo apt upgrade -y`.
2. Install Docker Engine + the Compose plugin (from Docker's official apt repository), then let your user run Docker without sudo:
   ```bash
   sudo usermod -aG docker $USER   # log out and back in afterwards
   docker run --rm hello-world      # proves Docker works
   docker compose version           # proves Compose v2 is installed
   ```
3. Install Tailscale and join your tailnet: `sudo tailscale up --ssh`.
4. Make it a 24/7 machine. If it is a laptop, keep it plugged in, then:
   ```bash
   sudo systemctl mask sleep.target suspend.target hibernate.target hybrid-sleep.target
   # /etc/systemd/logind.conf.d/stay-awake.conf
   [Login]
   HandleLidSwitch=ignore
   HandleLidSwitchExternalPower=ignore
   ```
5. Check resources: `free -h` (aim for 16 GB of RAM; 8 GB works with a smaller Spark), `df -h /` (50 GB+ free), `nproc`.

**Why:** Docker gives every service the same environment everywhere. Tailscale gives a private, encrypted network between your devices with no port forwarding on the home router. Masking sleep keeps scheduled jobs from being missed.

**Connects to:** every later step runs on this machine through `ssh <USER>@<SERVER>`.

---

## Step 2: Create the repository and the code workflow

**Do (on the dev PC):**
```bash
git clone https://github.com/<you>/spark-airflow-data-analyst-portfolio.git
cd spark-airflow-data-analyst-portfolio
git config user.name "Your Name"     # author for this repo only
```
Create this layout:
```
airflow/dags/          # Airflow pipeline definitions (DAGs)
src/atl_transit/       # plain-Python business logic, importable and testable
tests/                 # pytest tests for src/
docker/postgres/init/  # database bootstrap script
docker-compose.yml     # the platform
.env.example           # config template (committed); .env (secrets, NOT committed)
.gitignore
pyproject.toml         # Python project, pytest + ruff settings
.github/workflows/ci.yml
```

**Why this split:** the DAG file only *wires steps together*. The real logic (for example "is this GTFS zip valid?") lives in `src/`, which has no Airflow imports, so it can be unit-tested in milliseconds without starting Airflow. Interviewers look for exactly this separation.

**`.gitignore` must exclude** `.env` (passwords) and runtime data. The repo is public.

**Connects to:** Step 10 (CI tests `src/`) and Step 7 (Airflow mounts `airflow/dags` and `src/` into its containers).

---

## Step 3: Configuration and secrets (`.env`)

**Do:** copy the template and fill in real values. On the server only:
```bash
cp .env.example .env
python3 -c "import secrets; print(secrets.token_urlsafe(32))"            # for each password/secret
python3 -c "import base64,os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())"  # AIRFLOW_FERNET_KEY
chmod 600 .env
```

| Variable | Used by | Purpose |
|---|---|---|
| `BIND_ADDR` | all published ports | Which network interface the UIs listen on: `127.0.0.1` = server only; the Tailscale IP = your devices only |
| `AIRFLOW_UID` | Airflow containers | Run Airflow as your Linux user id (`id -u`) so file permissions match |
| `AIRFLOW_FERNET_KEY` | Airflow | Encrypts stored connection passwords in its database |
| `AIRFLOW_JWT_SECRET` | Airflow | Signs the tokens that Airflow's components use to talk to each other |
| `AIRFLOW_ADMIN_USER/PASSWORD` | airflow-init | The web UI login |
| `POSTGRES_SUPERUSER_PASSWORD` | postgres | The `postgres` admin account |
| `AIRFLOW_DB_PASSWORD`, `WAREHOUSE_DB_PASSWORD` | init script + Airflow | Separate least-privilege database users |
| `LAKE_ACCESS_KEY/SECRET_KEY` | lake + Airflow | S3-style credentials (like an AWS access key pair) |

**Why:** configuration is separated from code (the "12-factor" principle). The same `docker-compose.yml` works on any machine; only `.env` changes. Compose reads `.env` automatically and substitutes `${VAR}`. The syntax `${VAR:?message}` makes startup **fail loudly** if a secret is missing.

**Connects to:** every service in Step 4 reads its settings from here.

---

## Step 4: Anatomy of `docker-compose.yml`

Compose describes **services** (containers), **volumes** (persistent disks) and **networks**. Key ideas used in this file:

1. **Service names are hostnames.** Compose puts every service on one private network with built-in DNS. Airflow reaches the database at `postgres:5432` and the lake at `http://lake:9000`. These names only work *inside* that network.
2. **Published ports** (`"${BIND_ADDR}:8080:8080"` means host-interface:host-port:container-port) open a door from outside into one container, for your browser or Power BI. Internal traffic doesn't need them.
3. **Named volumes** (`postgres-data`, `lake-data`, `airflow-logs`) keep data when containers are deleted or upgraded. `docker compose down` keeps them; `docker compose down -v` **deletes** them.
4. **Bind mounts** (`./airflow/dags:/opt/airflow/dags:ro`) map repo folders into containers read-only. A `git pull` on the server makes new DAG code visible to Airflow immediately.
5. **Healthchecks + `depends_on: condition: service_healthy`** set the startup order. Postgres must accept connections before Airflow migrates its database, and the migration (`airflow-init`) must finish (`service_completed_successfully`) before the scheduler starts.
6. **YAML anchors** (`x-airflow-common: &airflow-common` reused with `<<: *airflow-common`) define the Airflow settings once and share them across its four services.
7. **Memory limits** (`deploy.resources.limits.memory`) stop one service from starving the others on a 16 GB machine.
8. **Pinned image versions** (`apache/airflow:3.4.0-python3.12`, never `latest`) make every rebuild identical.

Startup order this creates:
```
postgres (healthy) ─┐
lake (started) ─────┴─► airflow-init (migrate DB, create admin, exits 0) ─► apiserver + scheduler + dag-processor
spark-master ─► spark-worker            (independent of Airflow for now)
```

---

## Step 5: Postgres + PostGIS (the warehouse and Airflow's memory)

**Do:** nothing by hand. On the **first** start with an empty volume, the official Postgres image runs every script in `/docker-entrypoint-initdb.d/`. Ours (`docker/postgres/init/01-init.sh`):
1. creates role + database `airflow` (Airflow's metadata: DAG runs, task states, users),
2. creates role + database `warehouse` (analytics data),
3. enables the `postgis` extension in `warehouse` and creates schemas `raw`, `staging`, `marts`.

**Why one server with two databases:** it saves RAM. Separate roles keep the pipeline's data and Airflow's bookkeeping isolated (least privilege). Schemas follow the layered model dbt will fill: `raw` (as loaded) → `staging` (cleaned, renamed) → `marts` (star schema for BI).

**Important:** init scripts run **only once**. To re-run them you must delete the volume (`docker compose down -v`), which erases all data.

**Verify:**
```bash
docker compose exec postgres psql -U postgres -d warehouse -c "select postgis_version();" -c "\dn"
```
Expected: `3.7 ...` and the schemas `raw`, `staging`, `marts`.

---

## Step 6: The data lake (RustFS, S3-compatible object storage)

**What it is:** object storage. You store files ("objects") in **buckets** under **keys** that look like paths. RustFS implements the same HTTP API as Amazon S3, so any S3 client (boto3, Airflow's `S3Hook`, Spark's `s3a://`, the AWS CLI) works by changing only the endpoint URL. (MinIO used to be the usual choice; its official Docker image is no longer published, so this project uses RustFS.)

**Layout used (zones + Hive-style partitions):**
```
s3://raw/gtfs_static/ingest_date=2026-11-22/google_transit.zip   ← exact copy of the source
s3://raw/gtfs_static/ingest_date=2026-11-22/tables/stops.txt     ← each table extracted
s3://raw/gtfs_static/ingest_date=2026-11-22/_manifest.json       ← metadata: hash, row counts
s3://raw/gtfs_static/_latest.json                                ← pointer to the newest version
s3://curated/...                                                 ← Spark output (Phase 2)
```
- **raw zone:** never modified, so you can always reprocess from the original.
- **`key=value` folders** (`ingest_date=...`): Spark, Athena and DuckDB read these as a column and can skip folders a query doesn't need (partition pruning).

**Verify:** open the console at `http://<SERVER>:9001` and log in with `LAKE_ACCESS_KEY` / `LAKE_SECRET_KEY`.

---

## Step 7: Airflow 3 (the orchestrator)

**Components (each is its own container, all built from the same image):**

| Component | Role |
|---|---|
| **metadata DB** (Postgres `airflow` DB) | Airflow's memory: every DAG run, task state, retry, user |
| **dag-processor** | Reads the Python files in `dags/`, turns them into DAG definitions, stores them in the DB |
| **scheduler** | Decides which tasks are due (schedule + dependencies) and hands them to the **executor** |
| **executor: LocalExecutor** | Runs tasks as subprocesses inside the scheduler container. No Redis or Celery workers needed, which saves RAM. For multi-machine scale you'd switch to CeleryExecutor or KubernetesExecutor |
| **api-server** | Serves the web UI and REST API, plus the **Execution API** that running tasks call to report state. That's why `AIRFLOW__CORE__EXECUTION_API_SERVER_URL` points at it and every component shares `AIRFLOW__API_AUTH__JWT_SECRET` |
| **airflow-init** | One-shot: `airflow db migrate` (creates/updates tables) and creates the admin user, then exits |

**Configuration pattern:** any Airflow setting can be set with an env var named `AIRFLOW__<SECTION>__<KEY>`. For example `AIRFLOW__CORE__EXECUTOR=LocalExecutor` equals `[core] executor = LocalExecutor` in `airflow.cfg`.

Other choices in the file:
- `LOAD_EXAMPLES=false`: no demo DAGs cluttering the UI.
- `DAGS_ARE_PAUSED_AT_CREATION=true`: new DAGs don't run until you unpause them.
- `DEFAULT_TIMEZONE=America/New_York`: schedules are in Atlanta time.
- `PYTHONPATH=/opt/airflow/src`: DAGs can `import atl_transit`.
- `user: "${AIRFLOW_UID}:0"`: files Airflow writes belong to you, not root.

---

## Step 8: Connecting Airflow to the lake (Connections + Hooks)

**Pattern:** Airflow never hard-codes credentials. A **Connection** is a named credential record (`conn_id`), and a **Hook** is a Python class that uses it to talk to a system.

We define the connection with an environment variable, `AIRFLOW_CONN_<CONN_ID>`, holding JSON:
```json
{"conn_type": "aws", "login": "<access key>", "password": "<secret key>",
 "extra": {"endpoint_url": "http://lake:9000", "region_name": "us-east-1"}}
```
In code: `S3Hook(aws_conn_id="lake_s3")`. Because the connection type is `aws`, the same DAG talks to real Amazon S3 if you remove `endpoint_url` and use AWS keys. **This is the cloud-portability point.**

---

## Step 9: The first pipeline: `gtfs_static_weekly`

File: `airflow/dags/gtfs_static_weekly.py`. Written with the **TaskFlow API** (`@dag`, `@task` decorators).

```
download_and_validate ──► feed_changed (short-circuit) ──► upload
```

| Task | What it does | Pattern it demonstrates |
|---|---|---|
| `download_and_validate` | Streams the zip from MARTA to a temp folder, then calls `validate_gtfs_zip()` from `src/`: required files present, core tables non-empty, row counts, SHA-256 hash | **Fail fast**: a bad file raises an error, the task fails, and Airflow retries (2 retries, 10 min apart). Bad data never reaches the lake |
| `feed_changed` | Reads `_latest.json` from the lake and compares hashes. Returns `False` if identical | **Short-circuit**: downstream tasks are *skipped*, not failed. **Idempotency**: re-running never creates duplicates |
| `upload` | Writes the zip, each table, and the manifest, then updates `_latest.json` | **Immutable raw zone + manifest/lineage metadata** |

How data moves between tasks: the return value (a small dict) is passed through **XCom**, Airflow's store for small values between tasks, kept in the metadata DB. Large files are **not** passed through XCom. The task passes a file path instead, which works because LocalExecutor runs every task on the same machine. With distributed executors you'd stage files in the lake instead.

**Schedule:** `"0 6 * * 1"` (cron: 06:00 every Monday), `catchup=False` (don't backfill old weeks), `max_active_runs=1`.
Note: when you unpause it, Airflow immediately runs the most recent *completed* interval once. That's expected behavior.

**Real issue found while building:** MARTA's web server returns **HTTP 403** to Python's default `python-requests` User-Agent. The fix is to send an identifying `User-Agent` header. Debugging steps that found it: read the task log, then compare `curl -A "python-requests/2.32"` (403) with a custom agent (200).

**Run and verify:**
```bash
docker compose exec airflow-scheduler airflow dags list-import-errors     # expect: No data found
docker compose exec airflow-scheduler airflow dags unpause gtfs_static_weekly
docker compose exec airflow-scheduler airflow dags trigger gtfs_static_weekly
```
Then in the UI (`http://<SERVER>:8080`), open the DAG and look at the grid view: all green, or `upload` skipped if the feed was unchanged. In the lake console you should see `raw/gtfs_static/ingest_date=.../`.
Results of the first real run (Nov 2026 feed): 7,055 stops, 86 routes, 45,367 trips, **2,076,118 stop_times**, 364,651 shape points, 18.6 MB zip.

---

## Step 10: Tests and CI (GitHub Actions)

**Local:**
```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -e ".[dev]"
ruff check src tests airflow     # lint: style, unused imports, modern syntax
pytest -q                        # 5 tests: valid feed, missing file, no calendar, empty table, not-a-zip
```
The tests build tiny fake GTFS zips in a temporary folder (`tmp_path` fixture), so they need no network and no Airflow.

**CI** (`.github/workflows/ci.yml`) runs on every push and pull request: checkout → Python 3.12 → ruff → pytest → `docker compose config --quiet` (catches YAML and variable mistakes). A red X on GitHub means don't deploy.

---

## Step 11: Spark standalone cluster (ready for Phase 2)

- **spark-master**: the cluster manager. It tracks workers and hands out resources. UI at `http://<SERVER>:8081`.
- **spark-worker**: registers with `spark://spark-master:7077` and offers `SPARK_WORKER_CORES=4` and `SPARK_WORKER_MEMORY=2g` to jobs. UI at `:8082`.
- A **driver** (your PySpark program, submitted later by Airflow) asks the master for **executors** on the worker. Executors read from the lake with `s3a://`, transform the data in parallel, and write to `s3://curated/`.

Verify now: the master UI should list 1 worker, `ALIVE`, 4 cores, 2 GB.

---

## Step 12: Bring it all up, from zero

```bash
ssh <USER>@<SERVER>
git clone https://github.com/<you>/spark-airflow-data-analyst-portfolio.git
cd spark-airflow-data-analyst-portfolio
cp .env.example .env && nano .env        # Step 3
docker compose config --quiet             # validate
docker compose pull                       # download images (~7 GB the first time)
docker compose up -d                      # start in the background
docker compose ps                         # all "Up"; apiserver/scheduler/postgres "(healthy)"; airflow-init "Exited (0)"
```
Then Steps 5, 9 and 11 for verification.

## Step 13: Day-to-day operations

| Task | Command |
|---|---|
| Deploy new code | dev PC: `git push`, then server: `git pull && docker compose up -d` |
| Follow logs | `docker compose logs -f airflow-scheduler` |
| Restart one service | `docker compose restart lake` |
| Resource usage | `docker stats --no-stream` |
| Stop (keep data) | `docker compose down` |
| Wipe everything (deletes data!) | `docker compose down -v` |
| Upgrade a component | change its pinned tag in `docker-compose.yml`, commit, pull, `up -d` |

---

## Step 14: How to explain it in 60 seconds

> "I built a local-first data platform for Atlanta transit data that's portable to the cloud. Airflow 3 orchestrates the ingestion. The first DAG pulls MARTA's GTFS feed weekly, validates it with unit-tested Python, and uses a content hash to skip unchanged feeds, so it's idempotent. Raw files land immutable in an S3-compatible lake, partitioned by ingest date with a manifest for lineage. Spark will curate them into Delta/Parquet, and dbt models them into a PostGIS star schema for Power BI. Everything is defined in one Docker Compose file with pinned versions, health-checked startup order, memory limits and secrets in env vars, and GitHub Actions runs lint, tests and config validation on every push. Swapping in AWS S3, Databricks or Snowflake is a configuration change, not a rewrite."

---

# Phase 1: realtime capture and reference data

Phase 1 adds three flows on top of the foundation:

```
MARTA GTFS-rt ──(every 30 s / 120 s)──► rt-poller ──► s3://raw/gtfs_rt/...pb.gz        (raw snapshots)
                                                         │ hourly (Airflow)
                                                         ▼
                                         s3://raw/gtfs_rt_parquet/...parquet  (+ manifest per hour)
                                                         │ daily (Airflow): delete compacted snapshots > 14 days
Census TIGERweb ──(quarterly, Airflow)──► s3://raw/census_tracts/...geojson ──► Postgres raw.census_tracts (PostGIS)
```

## Step 15: Understand the realtime feeds

GTFS-realtime is **Protocol Buffers** (protobuf), a compact binary format defined by a schema (`gtfs-realtime.proto`). You need the generated Python classes (`gtfs-realtime-bindings`) to decode it. Each fetch returns a `FeedMessage` with a header (whose `timestamp` says when MARTA generated it) and a list of entities.

| Feed | URL path | Size per fetch (Sunday midday) | Content |
|---|---|---|---|
| Vehicle positions | `/vehicle/vehiclepositions.pb` | ~14 KB, ~171 vehicles | lat/lon, bearing, trip, route, vehicle id |
| Trip updates | `/tripupdate/tripupdates.pb` | ~850 KB, ~20,000 stop predictions | per trip: arrival/departure time at each upcoming (and just-passed) stop |

MARTA regenerates both every ~30 seconds. No API key is needed, but a real `User-Agent` is required (same as the static feed).

**Inspect one yourself:**
```python
import requests
from google.transit import gtfs_realtime_pb2
b = requests.get("https://gtfs-rt.itsmarta.com/TMGTFSRealTimeWebService/vehicle/vehiclepositions.pb",
                 headers={"User-Agent": "my-test/0.1"}).content
m = gtfs_realtime_pb2.FeedMessage(); m.ParseFromString(b)
print(m.header.timestamp, len(m.entity)); print(m.entity[0])
```

## Step 16: Capture with a dedicated poller service (not Airflow)

**Design decision:** Airflow is a *batch orchestrator*. Each DAG run creates database rows, logs and scheduling overhead. Running a DAG every 30 seconds (2,880 runs per day) would bloat the metadata DB and the scheduler. The standard pattern is:
- a small **long-running service** does the high-frequency capture, and
- **Airflow** does the periodic batch work on what was captured (hourly compaction).

`src/atl_transit/rt_poller.py`, run in its own container (`rt-poller`, image built from `docker/poller/Dockerfile`):
1. A loop with a separate timer per feed (vehicle positions every 30 s, trip updates every 120 s; set in `.env`).
2. Fetch → decode the header → **skip if the header timestamp equals the last one stored** (dedup: MARTA hadn't refreshed yet).
3. Gzip the bytes and `put_object` to `s3://raw/gtfs_rt/feed=<feed>/date=YYYY-MM-DD/hour=HH/<feed>_<unix_ts>.pb.gz`. The partition comes from the **feed's own UTC timestamp**, not the fetch time.
4. On error: log it and back off exponentially (capped at 5 min), but never exit. One bad response must not stop capture.
5. Write a **heartbeat** file every loop. The Docker `HEALTHCHECK` marks the container unhealthy if it's older than 3 minutes.
6. Handle `SIGTERM` (sent by `docker compose stop`) to exit cleanly.

The poller uses plain `boto3` (not Airflow hooks) because it isn't an Airflow component. Its image is a small `python:3.12-slim`, not the 3 GB Airflow image.

**Verify:** `docker compose logs -f rt-poller` shows `{"event": "stored", ...}` lines. In the lake console, `raw/gtfs_rt/` grows.

Storage maths (why compaction + retention): trip updates are ~190 KB gzipped × 720 per day ≈ 140 MB/day, and vehicle positions ≈ 15 MB/day.

## Step 17: Extend the Airflow image

The DAGs now import `gtfs-realtime-bindings`, which the official image doesn't include. `docker/airflow/Dockerfile` extends it:
```dockerfile
FROM apache/airflow:3.4.0-python3.12
RUN pip install --no-cache-dir "apache-airflow==3.4.0" "gtfs-realtime-bindings==2.0.0" "protobuf>=6,<7"
```
- Pinning `apache-airflow==3.4.0` in the same command stops pip from changing Airflow while resolving dependencies.
- `protobuf<7`: bindings 3.x need protobuf 7, which would conflict with Airflow's own dependencies, so we use bindings 2.0.0.
- In `docker-compose.yml` the shared Airflow block now has `build:` plus `image: atl-transit/airflow:3.4.0-1`. Run `docker compose build` once, and after any Dockerfile change.

Why not `_PIP_ADDITIONAL_REQUIREMENTS`? It reinstalls packages on **every** container start: slow, and it breaks if PyPI is unreachable. Baked images are reproducible.

## Step 18: Hourly compaction DAG (`gtfs_rt_hourly`)

**The "small files problem":** 120 + 30 tiny files per hour is inefficient for Spark and query engines (per-file overhead). Compaction merges each hour into **one columnar Parquet file per feed**.

```
wait_for_late_files ──► compact[vehicle_positions] ─┐
                    └─► compact[trip_updates] ──────┴─► check_and_record
```

Key techniques:
1. **Timetable with data intervals:** `CronDataIntervalTimetable("0 * * * *", timezone="UTC")`. The run for interval `[22:00, 23:00)` starts at 23:00 and processes exactly that hour. *Gotcha found while building:* in Airflow 3 a plain `schedule="@hourly"` uses `CronTriggerTimetable`, which runs **at** 22:00 with an empty interval, so it compacted a half-captured hour. Use the data-interval timetable whenever a run must cover "the period that just ended".
2. **Sensor:** `TimeDeltaSensor(delta=5 min, mode="reschedule")` waits until 23:05 so the last snapshots of the hour have landed. `reschedule` mode frees the worker slot between checks instead of sleeping in it.
3. **Dynamic task mapping:** `compact.expand(feed=[...])` creates one task instance per feed at runtime (`map_index` 0 and 1). They run in parallel and retry independently. Downstream, `check_and_record` receives all mapped results as a list. In Airflow 3 that's a lazy sequence, so convert it with `[dict(r) for r in results]`.
4. **Streaming write:** each snapshot is decoded, flattened (vehicle positions: 1 row per vehicle; trip updates: 1 row per trip × stop) and appended to a `pyarrow.parquet.ParquetWriter` (zstd compression). Memory stays small even for ~600k trip-update rows per hour.
5. **Explicit schema** (`VEHICLE_POSITIONS_SCHEMA`, `TRIP_UPDATES_SCHEMA` in `gtfs_rt.py`): unset protobuf fields become NULL, not 0. A missing speed is not a speed of zero.
6. **Idempotency:** output key `.../hour=HH/part-000.parquet` with `replace=True`, so re-running an hour overwrites it instead of duplicating.
7. **Data-quality check:** completeness = snapshots ÷ expected (3600 / poll interval). Zero snapshots fails the run (capture is down); under 80% is recorded as a warning.
8. **Manifest per hour:** `s3://raw/gtfs_rt_parquet/_manifests/date=.../hour=HH.json` holds counts, completeness and problems. It's the audit trail, and housekeeping uses it.
9. **catchup=True:** if the server was off for 6 hours, Airflow creates the 6 missed runs when it comes back. `max_active_runs=2` throttles them.

**Verify:** after HH:05 UTC, the grid view shows a green run. `raw/gtfs_rt_parquet/feed=.../date=.../hour=.../part-000.parquet` exists, and the manifest shows `completeness` near 1.0.

## Step 19: Retention DAG (`lake_housekeeping`)

Daily at 03:30 ET it deletes raw snapshots **older than 14 days AND whose hour has a manifest** (that is, it was compacted). Uncompacted hours are never deleted, so a broken compaction can't cause data loss. Deletes are batched (S3 `DeleteObjects` takes max 1,000 keys per call). The decision logic is a pure function (`purgeable_keys`) with a unit test.

## Step 20: Reference data, Census tracts into PostGIS (`census_tracts_reference`)

Source: the Census Bureau's **TIGERweb** ArcGIS REST service (no key). Region: the 11 Atlanta Regional Commission counties (FIPS state 13).
1. **Count first** (`returnCountOnly=true`) → 1,248 tracts.
2. **Page through** results (`resultOffset` / `resultRecordCount=200`, ordered by GEOID) as GeoJSON in WGS84 (`outSR=4326`).
3. **Verify** the number of unique GEOIDs equals the count; otherwise fail. That catches silent paging bugs.
4. **Land raw** GeoJSON in `s3://raw/census_tracts/ingest_date=.../` (raw-zone rule).
5. **Load in one transaction:** `CREATE TABLE IF NOT EXISTS` → `TRUNCATE` → insert all rows → validate the row count → `ANALYZE` → `COMMIT`. On any error, `ROLLBACK`. Readers never see an empty or half-loaded table (TRUNCATE takes a lock that makes them wait for the commit).
6. Geometry: `ST_Multi(ST_SetSRID(ST_GeomFromGeoJSON(...), 4326))` stores every tract as `MultiPolygon` in SRID 4326. A **GiST index** makes point-in-polygon joins fast.

New Airflow connection: `AIRFLOW_CONN_WAREHOUSE_PG` (JSON) → `PostgresHook(postgres_conn_id="warehouse_pg")`. It logs in as the `warehouse` role, not the superuser.

**Verify:**
```sql
SELECT count(*), count(DISTINCT county_fips), count(*) FILTER (WHERE NOT ST_IsValid(geom)) FROM raw.census_tracts;
-- 1248 | 11 | 0
SELECT geoid, tract_name FROM raw.census_tracts
WHERE ST_Contains(geom, ST_SetSRID(ST_MakePoint(-84.3916, 33.7539), 4326));   -- Five Points station
-- 13121011901 | Census Tract 119.01
```

*(Census ACS demographics such as population, income and car-free households need a free Census API key; they'll be added once one is available.)*

## Step 21: Deploy Phase 1

```bash
git pull
# add the new variables from .env.example to .env (POLL_*, RAW_RT_RETENTION_DAYS)
docker compose build            # builds atl-transit/airflow and atl-transit/rt-poller
docker compose up -d            # recreates Airflow containers, starts rt-poller
docker compose exec airflow-scheduler airflow dags unpause gtfs_rt_hourly
docker compose exec airflow-scheduler airflow dags unpause lake_housekeeping
docker compose exec airflow-scheduler airflow dags unpause census_tracts_reference
docker compose exec airflow-scheduler airflow dags trigger census_tracts_reference
```

## Updated 60-second pitch

> "...On top of that, a dedicated poller captures MARTA's GTFS-realtime protobuf feeds every 30 seconds, deduplicated by feed timestamp, into the raw zone. Airflow compacts each hour into Parquet using a data-interval timetable, a late-arrival sensor and dynamic task mapping per feed, with a completeness check and a manifest per hour. Retention deletes raw snapshots only after they've been compacted. Census tract boundaries are loaded transactionally into PostGIS for spatial joins. I chose a separate poller because Airflow is a batch orchestrator, not a 30-second scheduler."

---

# Phase 2: Spark curation into Delta Lake (and Census demographics)

```
raw zone (Parquet, per hour) ──► Spark (via Spark Connect) ──► curated zone (Delta Lake tables)
   gtfs_static/.../tables/*.txt ─────────────────────────────►  curated/gtfs_static/<table>
   gtfs_rt_parquet/feed=vehicle_positions/... ───────────────►  curated/vehicle_positions
   gtfs_rt_parquet/feed=trip_updates/...  + stop_times ──────►  curated/stop_arrivals  (observed vs scheduled)
Census API (ACS 5-year) ──► raw/census_acs5/... ──► Postgres raw.acs_tract_demographics
```

## Step 22: Choose how Airflow talks to Spark (Spark Connect)

Options considered:
| Option | Problem here |
|---|---|
| `SparkSubmitOperator` from the Airflow container | Needs Java + Spark in the Airflow image (big), and the **driver's Python version must match the executors'** (Airflow uses 3.12, the Spark image 3.10) |
| `DockerOperator` (start a Spark container per job) | Mounting the Docker socket into Airflow is effectively root on the host |
| **Spark Connect** (chosen) | Spark 4's client/server API. A long-running **Spark Connect server** is the driver. Clients send DataFrame plans over gRPC (port 15002) using the thin `pyspark-client` package: no JVM, no Python-version coupling |

How it fits together:
```
Airflow task (pyspark-client) ──gRPC plan──► spark-connect (driver, 1 GB) ──► spark-master ──► spark-worker executor (4 cores, 1.5 GB)
                                                     │                                              │
                                                     └──────────── s3a:// read/write ───────────────┴──► RustFS lake
```
In code: `SparkSession.builder.remote("sc://spark-connect:15002").getOrCreate()` (see `src/atl_transit/spark_session.py`).

## Step 23: Build a Spark image with Delta Lake and S3 support

`docker/spark/Dockerfile` extends `apache/spark:4.2.1-python3` and downloads jars into `/opt/spark/jars` **at build time**:

| Jar | Why | Version rule |
|---|---|---|
| `hadoop-aws` | The `s3a://` filesystem | Must equal Spark's bundled Hadoop (`hadoop-client-api-3.5.0.jar` → 3.5.0) |
| `software.amazon.awssdk:bundle` | AWS SDK v2 used by S3A | The version Hadoop was built with (`aws-java-sdk-v2.version` in the hadoop-project POM → 2.35.4) |
| `analyticsaccelerator-s3` | Required by Hadoop 3.5's S3A input stream | From the same POM (1.3.1). Without it reads fail with `NoClassDefFoundError: .../analyticsaccelerator/...` |
| `delta-spark_2.13`, `delta-storage` | Delta Lake | Delta 4.4.1 supports Spark 4.2 |

**How to find compatible versions yourself:** look at which Hadoop jars the Spark image ships (`ls /opt/spark/jars | grep hadoop`), then read that Hadoop version's `hadoop-project` POM on Maven Central for its dependency versions.

## Step 24: Configure Spark Connect for the lake and Delta (`docker-compose.yml` → `spark-connect`)

| Setting | Purpose |
|---|---|
| `--master spark://spark-master:7077` | Use the standalone cluster |
| `spark.driver.host=spark-connect` | Executors must reach the driver by its service name |
| `spark.executor.memory=1536m`, `cores=4` | Fits inside the worker's 2 GB / 4 cores |
| `spark.sql.shuffle.partitions=8` | Default is 200, far too many tiny tasks for hourly data. **A tuning point worth mentioning** |
| `spark.sql.session.timeZone=UTC` | Timestamps are stored and computed in UTC; local time only where explicitly needed |
| `spark.sql.extensions` / `spark_catalog` = Delta classes | Enables Delta SQL (`MERGE INTO`, `DESCRIBE HISTORY`, `delta.\`path\``) |
| `fs.s3a.endpoint=http://lake:9000`, `path.style.access=true`, `ssl.enabled=false` | Point S3A at RustFS instead of AWS |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` env vars | S3A's default credential chain reads them, so secrets stay out of process arguments |

The same env vars go on the **worker** too, because executors read and write the lake directly.

**Verify:** `docker compose logs spark-connect | grep "server started"`, and the Spark master UI (:8081) shows the "Spark Connect server" application with one executor.

## Step 25: Profile the data before writing transformations

Before writing jobs, test the join keys you're about to rely on:
- All 918 realtime `trip_id`s exist in the static `trips.txt`. ✔
- Realtime `stop_id`s are **internal codes**: only 1,842 of 6,565 exist in `stops.txt`. ✘
- `(trip_id, stop_sequence)` matches `stop_times.txt` **100%**. ✔ → this is the join key, and the official `stop_id` comes from the schedule.
- GTFS times go past 24:00 (`26:11:00` = 2:11 AM the next calendar day, same service day).

Interview point: *"I found the realtime stop IDs didn't match the schedule, so I profiled the join keys and switched to trip plus stop sequence, which matched 100%."*

## Step 26: Write the transformations (`src/atl_transit/spark_jobs.py`)

Two layers:
1. **Pure transformations** (`prepare_*`: DataFrame in, DataFrame out). They're unit-tested with a local SparkSession and need no lake.
2. **I/O** (`curate_*`, `merge_*`): read the lake, MERGE into Delta.

| Curated table | Logic |
|---|---|
| `gtfs_static/<table>` | Typed columns from a spec (missing optional columns become typed NULLs, so the schema is stable). Dates parsed. `arrival_secs` = seconds from the GTFS "HH:MM:SS" (handles ≥24 h). Full overwrite; Delta keeps old versions |
| `vehicle_positions` | Reject rows with no vehicle id, timestamp or position, or outside the Atlanta bounding box. Deduplicate per `(vehicle_id, vehicle_ts)` (the same GPS fix repeats in each 30 s snapshot) with a `row_number()` window. `event_date` in Atlanta local time. Partitioned by `event_date` |
| `stop_arrivals` | From trip updates, keep rows where `arrival_time <= feed_ts` (the stop has already been served). The latest such report per `(start_date, trip_id, stop_sequence)` is the **observed arrival**. Join to `stop_times` → **scheduled arrival** using the GTFS rule *"noon minus 12 hours, local time"* (correct on DST change days) → `delay_s`. Partitioned by `service_date` |

**Assumption to validate later (Phase 4):** MARTA's reports for passed stops reflect actual arrival times. Cross-check against vehicle GPS positions.

## Step 27: Make writes idempotent with Delta MERGE

```sql
MERGE INTO delta.`s3a://curated/stop_arrivals` t
USING arrivals_src s
ON  t.service_date IN (DATE'2026-11-23')          -- partition pruning: only touch today's files
AND t.service_date = s.service_date AND t.trip_id = s.trip_id AND t.stop_sequence = s.stop_sequence
WHEN MATCHED AND s.feed_ts > t.feed_ts THEN UPDATE SET *   -- newer report wins
WHEN NOT MATCHED THEN INSERT *
```
- **Upsert:** a stop passed at 10:59 shows up in both the 10:00 and 11:00 hours. The second run *updates* instead of duplicating. In the first backfill, ~650-730 rows per hour were updates.
- **Order-independent:** because of `s.feed_ts > t.feed_ts`, processing hours in any order (retries, backfills) gives the same final table.
- **ACID + snapshot isolation:** readers never see a half-written MERGE, and a reader of `stop_times` during an overwrite sees the old or the new version, never a mix.
- `DESCRIBE HISTORY delta.\`...\`` shows every MERGE with its inserted/updated row counts (audit trail, time travel).

## Step 28: Orchestrate with Airflow Assets (data-aware scheduling)

`airflow/dags/lake_assets.py` defines Assets (URIs that name datasets). A producer task declares `outlets=[ASSET]`, and a consumer DAG uses `schedule=[ASSET]`:

```
gtfs_static_weekly.upload ──(RAW_GTFS_STATIC)──► spark_curate_gtfs_static ──(CURATED_GTFS_STATIC)
gtfs_rt_hourly.check_and_record ──(RAW_RT_PARQUET)──► spark_curate_rt ──(CURATED_VEHICLE_POSITIONS, CURATED_STOP_ARRIVALS)
```
- Events fire only on **success**. An unchanged static feed (upload skipped) or an hour with no data (check fails) triggers nothing downstream.
- `spark_curate_rt` is **incremental with a checkpoint**: it processes every hour that has a manifest with data but isn't yet in `s3://curated/_state/rt_curated_hours.json`, oldest first, max 24 per run, and checkpoints after each hour. Missed triggers, manual runs and backfills all converge to the same state.
- `max_active_runs=1`: concurrent MERGEs into the same Delta table would conflict.

**Run it:**
```bash
docker compose exec airflow-scheduler airflow dags unpause spark_curate_gtfs_static
docker compose exec airflow-scheduler airflow dags unpause spark_curate_rt
docker compose exec airflow-scheduler airflow dags trigger spark_curate_gtfs_static   # first time only
docker compose exec airflow-scheduler airflow dags trigger spark_curate_rt            # backfills all captured hours
```

## Step 29: Test PySpark code

`tests/spark/test_spark_jobs.py` uses `SparkSession.builder.master("local[2]")`:
- GTFS time parsing past midnight; typed static tables with missing columns.
- Vehicle dedup keeps the earliest sighting; out-of-area and id-less rows are rejected; local event date.
- Stop arrivals: latest passed report wins, future stops are excluded, the schedule join gives the delay, unscheduled trips are flagged.
- **DST day:** 10:00 local on 2026-03-08 = 14:00 UTC.

Tips: give `createDataFrame` an explicit schema when a column is all NULL (types can't be inferred). Tests skip automatically without Java. CI installs Java 21 (`actions/setup-java`).

## Step 30: Census ACS demographics (`census_tracts_reference`, second branch)

- Needs a free key (api.census.gov), stored **only** in the server `.env` as `CENSUS_API_KEY` and passed to Airflow through an env var. If it isn't set, the task **skips**.
- One request for all 11 counties: `get=NAME,<variables>&for=tract:*&in=state:13&in=county:057,063,...`
- Variables: total population, median household income, workers / transit commuters, households / zero-vehicle households (owner + renter), poverty.
- ACS uses negative sentinels (`-666666666` = not available), which become NULL.
- **Secret hygiene:** HTTP library errors include the full URL, which contains the key. The code re-raises a clean error `from None`, and there's a unit test proving the key isn't in the message.
- Same transactional full-refresh pattern → `raw.acs_tract_demographics`. Verified: 1,248 tracts, every one joins to a boundary, region population 5.06 M.

## Phase 2 results (first backfill, 10 captured hours)

| Metric | Value |
|---|---|
| Curated vehicle positions | 92,373 (225 vehicles) |
| Observed stop arrivals | 37,979 (0 duplicate keys) |
| On time (−1 to +5 min) | 69.1% |
| Late > 5 min / early > 1 min | 11.3% / 19.7% |
| Median delay | 0.7 min |

## Updated 60-second pitch (add)

> "...Spark 4 runs through Spark Connect, so Airflow sends DataFrame plans over gRPC without carrying a JVM. PySpark jobs dedupe GPS fixes and derive observed-vs-scheduled arrivals, joined on trip and stop sequence after profiling showed the realtime stop IDs didn't match the schedule. Results land in Delta Lake through partition-pruned, order-independent MERGEs, so backfills and retries are idempotent. The DAGs are chained with Airflow Assets, and the realtime curation is incremental with a checkpoint. First result: 69% of observed arrivals were on time."

---

# Phase 3: the warehouse (Postgres/PostGIS) and dbt

```
curated Delta ──Spark JDBC──► raw._load_* ──Airflow, 1 transaction──► raw.gtfs_*, raw.rt_stop_arrivals
Census DAG ──────────────────────────────────────────────────────────► raw.census_tracts, raw.acs_tract_demographics
                                                                            │  dbt build (runs + tests)
                                                                            ▼
                                     staging (views) ──► intermediate (spatial join) ──► marts (star schema + KPIs)
```

## Step 31: Load the lake into the warehouse (Spark JDBC + atomic publish)

1. **Add the Postgres JDBC driver** to the Spark image (`postgresql-42.7.15.jar`). The *executors* open the database connections, so the driver must be on the cluster, not on the client.
2. **Spark writes a load table** (`raw._load_<name>`, mode overwrite = drop and recreate) with `batchsize=10000` and `numPartitions=4` (at most 4 parallel connections).
3. **Airflow publishes it in one transaction** (`src/atl_transit/warehouse_sql.py`):
   - GTFS static (**full refresh**): `TRUNCATE target; INSERT INTO target SELECT * FROM load; CREATE INDEX IF NOT EXISTS ...; DROP load; ANALYZE`
   - Realtime arrivals (**lookback window**, 3 days): `DELETE FROM target WHERE service_date >= cutoff; INSERT ... FROM load`. Recent days can still change (late MERGE updates), older days can't.
4. Why `TRUNCATE`, not `DROP`: dbt views are built on these tables, and Postgres won't drop a table that views depend on. Why a load table: Spark's JDBC write isn't one transaction (each partition commits separately). Staging it and publishing in SQL makes the switch atomic for readers.
5. Vehicle positions stay in the lake (hundreds of thousands of rows a day; analysed with Spark). Not everything belongs in the warehouse.

DAGs: `warehouse_load_gtfs_static` (triggered by the curated static asset) and `warehouse_load_rt` (triggered by curated stop arrivals). Each emits a warehouse asset when it finishes.

## Step 32: Install dbt without breaking Airflow

dbt-core pins its own versions of shared libraries (jinja, click, protobuf...). Installing it into Airflow's environment risks conflicts, so the Airflow image gets a **separate virtualenv**:
```dockerfile
RUN python -m venv /home/airflow/dbt-venv \
 && /home/airflow/dbt-venv/bin/pip install "dbt-core==1.12.7" "dbt-postgres==1.11.0"
```
Airflow calls `/home/airflow/dbt-venv/bin/dbt` from a `BashOperator`. The project folder is mounted read-only at `/opt/airflow/dbt`, so build artifacts go to `/tmp` via `DBT_TARGET_PATH` / `DBT_LOG_PATH`.

## Step 33: The dbt project (`dbt/`)

| File | Role |
|---|---|
| `dbt_project.yml` | Project config: folder → schema + materialization; **vars** for business rules (on-time window −60 s … +300 s, lookback 3 days, max plausible delay 3 h) |
| `profiles.yml` | Connection, **all from env vars** (`DBT_PG_HOST/USER/PASSWORD/DBNAME`). Targets `prod` (Airflow) and `ci` (GitHub Actions) |
| `macros/generate_schema_name.sql` | Use `staging` / `marts` as-is (dbt's default would create `staging_marts`) |
| `models/staging/_sources.yml` | Declares the raw tables, with **source freshness** on `rt_stop_arrivals` (warn after 3 h, error after 12 h) |

**Layers:**
| Layer | Materialization | Models |
|---|---|---|
| staging | view | `stg_gtfs__routes/stops/trips/stop_times`, `stg_rt__stop_arrivals`, `stg_census__tracts/acs`. Rename, type, build PostGIS points (`ST_MakePoint(lon, lat)`), convert timestamps to `timestamptz`, derive ACS percentages |
| intermediate | table | `int_stops__with_tract`: **point-in-polygon** with `ST_Intersects` + `DISTINCT ON` (one tract per stop) |
| marts | table | `dim_route`, `dim_stop` (+ tract), `dim_tract` (+ ACS, GiST index), `dim_date` (`generate_series`), **`fct_stop_arrival`** (incremental), `mart_route_on_time_daily`, `mart_on_time_by_hour`, `mart_tract_service_equity` |

**The fact table:** grain = one observed arrival per (service_date, trip, stop_sequence). It's `incremental` with `incremental_strategy='delete+insert'` and `unique_key` = the grain: each run rebuilds only the last 3 service dates. Columns include `delay_s`, `is_on_time / is_late / is_early` (from the vars) and the scheduled hour in Atlanta local time.

**Tests (32):** built-in `unique`, `not_null`, `accepted_values` and `relationships` (fact → each dimension), plus two **custom generic tests** written as macros (`unique_combination` for composite keys, `accepted_range`), plus a **singular test** (`tests/assert_observed_stops_mostly_mapped_to_tracts.sql`, warn level). `dbt build` runs each model, then its tests, in dependency order, and a failing test skips everything downstream.

## Step 34: Orchestrate dbt with Assets

`dbt_build` runs on `WAREHOUSE_GTFS_STATIC | WAREHOUSE_RT_ARRIVALS | WAREHOUSE_CENSUS` (the `|` makes an **AssetAny**: any one update triggers it). Tasks: `dbt build --target prod` → `dbt source freshness` (`trigger_rule="all_done"`, so freshness is reported even if a model failed).

*First-run race observed:* on an empty warehouse, the realtime load finished first and triggered dbt before the GTFS tables existed, so the first attempt failed with "relation does not exist" and the automatic retry succeeded. It can only happen on the very first load.

## Step 35: Test dbt in CI against a real database

`.github/workflows/ci.yml` → job `dbt`:
1. A **PostGIS service container** (`postgis/postgis:17-3.7`) with a health check.
2. `psql -f ci/seed_warehouse.sql`: tiny fixtures with the same columns as production (2 routes, 2 stops (one inside a tract polygon, one outside), 3 scheduled arrivals that are on time / late / early, 1 unscheduled, 1 tract with ACS).
3. `dbt build --target ci` **twice** (the second run exercises the incremental path).
4. `psql -f ci/assert_marts.sql`: **known-answer checks** (3 fact rows, the 1/1/1 classification, the spatial join result, 41.4% car-free, 13:00 UTC = 08 local). Each check divides by zero on failure, so `ON_ERROR_STOP` fails the job.

**Replay CI locally** (same steps, throwaway containers):
```bash
docker network create dbtci
docker build -t atl-transit/postgres:17-3.7-h3 docker/postgres     # PostGIS + H3 (Phase 4)
docker run -d --name dbtci-pg --network dbtci -e POSTGRES_USER=ci -e POSTGRES_PASSWORD=ci -e POSTGRES_DB=warehouse atl-transit/postgres:17-3.7-h3
docker exec -i dbtci-pg psql -U ci -d warehouse < ci/seed_warehouse.sql
docker run --rm --network dbtci -v $PWD/dbt:/dbt:ro -e DBT_PG_HOST=dbtci-pg -e DBT_PG_USER=ci -e DBT_PG_PASSWORD=ci \
  -e DBT_PG_DBNAME=warehouse -e DBT_PROFILES_DIR=/dbt -e DBT_PROJECT_DIR=/dbt -e DBT_TARGET_PATH=/tmp/t -e DBT_LOG_PATH=/tmp/l \
  --entrypoint /home/airflow/dbt-venv/bin/dbt atl-transit/airflow:3.4.0-3 build --target ci
docker rm -f dbtci-pg && docker network rm dbtci
```

## Step 36: Deploy without overloading the server

New images for Spark and Airflow at once. Roll them out **in stages** (Spark services → `airflow-init` → the rest) and watch `free -h` between steps, instead of recreating every container at the same moment.

## Phase 3 results (first full build)

| | |
|---|---|
| dbt build | **48 / 48 pass** (models + tests), source freshness PASS |
| Fact rows | 42,653 observed arrivals (2026-11-22 → 23) |
| Stops mapped to tracts | 7,055 / 7,055 |
| Bus on-time (−1 / +5 min) | **70.4%** |
| Least reliable routes (≥300 arrivals) | 20 (27.1%), 11 (46.0%), 165 (47.8%) |
| Tracts with ≥20% car-free households | 73.4% on time vs 69.8% for <10% (preliminary: ~1 day of data) |

**Data limitation found:** MARTA's GTFS-realtime feed covers **buses only** (71 of 81 bus routes). The 4 rail lines and the Atlanta Streetcar don't appear in either realtime feed, so every on-time figure is a bus figure.

## Updated 60-second pitch (add)

> "...Spark loads curated tables into Postgres over JDBC into staging tables, and Airflow publishes them in a single transaction. dbt then builds a tested star schema: staging views, a PostGIS point-in-polygon join that puts every stop in its Census tract, an incremental fact table of observed arrivals, and marts for on-time performance by route, hour and neighborhood demographics. 32 data tests plus source freshness run on every build, and CI runs the whole dbt project twice against a real PostGIS container with known-answer assertions."

---

# Phase 4: Geospatial + BI

Goal: answer *where* buses are slow and lose time, check whether the feed's "arrival" is real, and serve the
results to people: a **public web map**, a **Power BI** report over a read-only login, and published **dbt docs**.

```
curated Delta (lake) ──Spark──> raw.rt_vehicle_pings ─┐                 ┌─> web/data/*.geojson ─> GitHub Pages map
                     └─Spark──> raw.rt_arrival_gps_check ┼─ dbt (PostGIS + H3) ─> marts ─┤
existing raw tables ─────────────────────────────────┘                 └─> Power BI (role `reporting`, via Tailscale)
```

## Step 37: Validate the "passed stop = arrival" assumption with GPS (Spark)

Phase 2 took an arrival as "the last time the trip-updates feed reported this stop with a time in the past".
18% of arrivals came out *early*, which is suspicious. The vehicle-positions feed gives an independent check: the bus's own GPS track.

`prepare_arrival_gps_check(arrivals, pings, stops)` in `src/atl_transit/spark_jobs.py`:
1. Join each arrival to its stop's coordinates.
2. **Range join** to the same trip's GPS fixes within ±20 minutes of the reported time:
   `a.trip_id = p.trip_id AND p.ping_epoch BETWEEN a.obs_epoch - 1200 AND a.obs_epoch + 1200` (left join, so arrivals without GPS survive).
3. **Haversine distance** from each fix to the stop, written as a plain column expression (`sin`, `cos`, `asin`, `sqrt`): no Python UDF, so it runs on the JVM through Spark Connect.
4. `groupBy(arrival).agg(count, min(dist), min_by(ping_epoch, dist))`. `min_by` returns the time of the *closest* fix in one pass, with no window function.
5. `gps_offset_s` = reported arrival − closest-fix time. `gps_confirmed` = a fix came within 150 m of the stop. Fixes arrive ~30 s apart, so the closest one is within ~15 s of the true pass.

`prepare_vehicle_pings(vp)`: `lag()` over `Window.partitionBy("vehicle_id").orderBy("vehicle_ts")` gives the previous fix. Speed = distance / time, kept only when it's the **same trip**, the gap is 5–300 s and the speed is ≤ 35 m/s. Otherwise it's NULL, so GPS jumps and layovers don't skew averages.

**Prototype first:** before wiring anything into Airflow, run the functions ad hoc through Spark Connect (`get_spark()` from inside the Airflow container) and look at the numbers. That's how the finding below was spotted.

## Step 38: Load GPS tables daily (time-based DAG)

`warehouse_load_rt_gps` (in `airflow/dags/warehouse_load.py`), cron `30 4 * * *` in `America/New_York`:
- `spark_export`: `export_rt_gps_window` writes `raw._load_rt_vehicle_pings` and `raw._load_rt_arrival_gps_check` for dates ≥ today − 2.
- `publish`: both lookback-window refreshes run in **one transaction**, so the two tables always cover the same days. Emits the `WAREHOUSE_RT_GPS` asset, which is added to `dbt_build`'s AssetAny.

Why cron and not an asset trigger: re-reading days of GPS fixes is too heavy to repeat every hour, and 04:30 is MARTA's quietest time. **Mixing time-based and data-aware schedules** in one pipeline is normal: the asset event still tells dbt when to rebuild.

## Step 39: Add H3 to Postgres (custom image)

**H3** (Uber's hierarchical hexagonal grid) gives every point on Earth a cell id at 16 resolutions. Hexagons have equal-distance neighbors and near-equal areas, which makes them better than squares or tracts for aggregating movement. Resolution 8 ≈ 0.74 km², edge ≈ 460 m.

`docker/postgres/Dockerfile`:
```dockerfile
FROM postgis/postgis:17-3.7
RUN apt-get update && apt-get install -y --no-install-recommends postgresql-17-h3 && rm -rf /var/lib/apt/lists/*
```
The official Postgres images already have the **PGDG apt repository**, which packages `h3-pg`. Check that it's available first: `docker exec <pg> bash -c "apt-get update -qq; apt-cache policy postgresql-17-h3"`.

`docker/postgres/init/02-warehouse-extras.sh` (idempotent; also run by hand on an existing volume):
- `CREATE EXTENSION h3; CREATE EXTENSION h3_postgis CASCADE;` Extensions run C code, so a superuser installs them. After that, the `warehouse` role just calls the functions.
- The read-only role (Step 41).

Functions used: `h3_lat_lng_to_cell(geometry, 8)` (point → cell) and `h3_cell_to_boundary_geometry(cell)` (cell → hexagon polygon). The `geometry` overloads come from `h3_postgis` and avoid the ambiguous (lat, lng) vs (x, y) order of the plain `point` type.

**Deploying it** (data volume kept):
```bash
docker compose build postgres && docker compose up -d postgres      # recreate with the new image, same volume
docker compose exec postgres bash /docker-entrypoint-initdb.d/02-warehouse-extras.sh
```

## Step 40: dbt models for Phase 4

| Model | What it does |
|---|---|
| `stg_rt__vehicle_pings` | km/h, `event_at` timestamptz, `h3_cell` |
| `stg_rt__arrival_gps_check` | typed GPS check |
| `dim_stop` (+ `h3_cell`) | every stop's hexagon, to join stops to GPS speeds |
| `fct_vehicle_ping` | incremental, `unique_key='event_date'` + delete+insert = **replace whole days**; `is_stopped` (< 2 km/h), `is_weekday_peak` (vars) |
| `mart_h3_bus_speed` | per hexagon: median *moving* speed (all day and weekday peak; `percentile_cont ... FILTER (WHERE ...)`), % of samples stopped, on-time % of the stops inside, hexagon polygon |
| `mart_segment_delay` | `lag()` over each trip's arrivals by `stop_sequence`. For **truly consecutive** stops, delay added = delay here − delay at the previous stop. Grouped per directed stop pair, with `string_agg(DISTINCT route ...)` and a line geometry (`ST_MakeLine`) |
| `mart_gps_validation` | **confusion matrix**: the feed's class (early / on time / late) × the class using the GPS pass time (`delay_s − gps_offset_s`), or `not_confirmed` |

Also: `+grants: {select: ["reporting"]}` on the marts folder. dbt re-applies grants after every rebuild, so a recreated table never loses its BI permission. New tests: composite uniqueness on pings / segments / the matrix, a speed range, freshness on pings (warn 30 h, error 54 h for a daily load).

## Step 41: Read-only `reporting` role for BI

In `02-warehouse-extras.sh`:
```sql
SELECT 'CREATE ROLE reporting LOGIN' WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'reporting') \gexec
ALTER ROLE reporting WITH LOGIN PASSWORD :'reporting_pw';
ALTER ROLE reporting SET default_transaction_read_only = on;   -- belt and braces
ALTER ROLE reporting SET statement_timeout = '120s';           -- a bad BI query can't hog the laptop
GRANT CONNECT ON DATABASE warehouse TO reporting;
GRANT USAGE ON SCHEMA marts TO reporting;                      -- table SELECT comes from dbt grants
```
- `\gexec` runs the generated statement: an idempotent `CREATE ROLE` (Postgres has no `CREATE ROLE IF NOT EXISTS`).
- The password lives in `.env` (`REPORTING_DB_PASSWORD`) and reaches the container as an env var. The psql variable `:'reporting_pw'` quotes it safely.
- **Least privilege in layers:** schema USAGE + table SELECT only on `marts`, a read-only session default, a timeout, and network access only over Tailscale.

## Step 42: Power BI report

Step-by-step in [`bi/powerbi/README.md`](../bi/powerbi/README.md): connect (PostgreSQL connector, Import mode, user `reporting`, server = Tailscale IP), remove geometry columns in Power Query, build the star in Model view (single-direction relationships, mark `dim_date` as a date table), add the DAX measures from [`bi/powerbi/measures.dax`](../bi/powerbi/measures.dax), apply the theme, build 5 pages, and save as **.pbip** (text-based, diffable in git).

## Step 43: Public web map (MapLibre on GitHub Pages)

- **Export** (`src/atl_transit/webmap.py`): PostGIS builds each GeoJSON `FeatureCollection` itself (`json_build_object` + `json_agg` + `ST_AsGeoJSON(geom, 5)` = 5 decimals ≈ 1 m). Tract polygons are simplified (`ST_SimplifyPreserveTopology`). Minimum sample sizes (≥30 speed samples per hexagon, ≥15 traversals per segment, ≥50 arrivals per tract) keep noise off the map. `summary.json` holds the headline numbers.
- **Snapshot** (`scripts/refresh_webmap_data.sh`): runs the export *on the server*, inside the Airflow container (which can reach Postgres), and streams the files back over SSH as a tar archive. Then commit `web/data/` and push.
  ```bash
  ATL_SERVER=user@host scripts/refresh_webmap_data.sh && git add web/data && git commit -m "Map snapshot" && git push
  ```
- **Page** (`web/`): plain HTML + JS, MapLibre GL JS 4.7.1 from a CDN, a free vector basemap (OpenFreeMap, no API key). Three layers (hexagon speed, segment delay lines, tract choropleth with an on-time / car-free toggle), hover tooltips, legends, headline stats and the worst-routes table. Data layers sit **under** the basemap labels (`addLayer(..., firstSymbolLayerId)`).
- **Colors:** a single-hue blue ramp for magnitudes (dark = worse) and blue ↔ gray ↔ red for gaining vs losing time, both checked for lightness steps and color-blind safety.
- **Testing it headless:** headless Chrome's `--virtual-time-budget` never finishes loading the basemap. Instead, drive Chrome over the DevTools protocol (`--remote-debugging-port`), wait in real time until `map.loaded()`, check `queryRenderedFeatures().length > 0`, then take a screenshot.

## Step 44: Publish the map + dbt docs from CI (GitHub Pages)

`.github/workflows/ci.yml`:
- The `dbt` job no longer uses a service container: it **builds `docker/postgres`** and runs it, so CI has exactly production's extensions (PostGIS + H3). Fixtures now include GPS pings and GPS checks, and `ci/assert_marts.sql` checks the new known answers (the "early" arrival that GPS shows was on time, a 480 s segment delay, the hexagon's 3 fixes / 50% stopped / 12 km/h / 2 arrivals, and the `reporting` grants).
- After the tests: `dbt docs generate --static` → one self-contained HTML file (manifest + catalog inlined). The job assembles `_site/` = `web/` + `dbt/index.html` and uploads it with `actions/upload-pages-artifact`.
- A `pages` job (`needs: [test, dbt]`, only on pushes to `main`, permissions `pages: write` + `id-token: write`) runs `actions/deploy-pages`. Turn Pages on once with **Settings → Pages → Source: GitHub Actions** (or `gh api -X POST repos/<owner>/<repo>/pages -f build_type=workflow`).

Live: https://cursorcanis.github.io/spark-airflow-data-analyst-portfolio/ (map) and `/dbt/` (docs + lineage graph).

## Step 45: Operations gotchas from Phase 4

1. **Stale bind mount.** After a `git pull`, the Airflow container saw an **empty** `/opt/airflow/dbt`: the directory had been replaced on the host (new inode) and the container still held the old, deleted one. Fix: `docker compose up -d --force-recreate --no-deps airflow-scheduler airflow-dag-processor airflow-apiserver`. Check with `docker compose exec airflow-scheduler ls /opt/airflow/dbt`.
2. **Order of first deploy:** pause `dbt_build`, run the new load once (`airflow dags trigger warehouse_load_rt_gps`), then unpause and trigger dbt. This avoids the Phase 3 "relation does not exist" race for the new sources.
3. **Python version of the Spark image is 3.10:** `datetime.UTC` (3.11+) doesn't exist there. Pipeline code shipped to executors must stay 3.10-compatible (tests run on 3.12 in CI).

## Phase 4 results

| | |
|---|---|
| dbt build | **63 / 63 pass** |
| Arrivals / GPS fixes | 81,461 arrivals; 197,633 GPS fixes (2026-11-22 → 23) |
| GPS confirmation | 76.3% of arrivals have a GPS fix within 150 m of the stop |
| **Feed vs GPS** (confirmed arrivals) | early **16.7% → 10.3%**, late **15.9% → 20.1%**, on time 67.4% → 69.6%. The feed's "passed" times run ahead of the bus (median −15 s, −126 s for "early" arrivals that GPS shows on time). |
| Speed | 1,357 hexagons with data. Moving speed within 5 km of Downtown/Midtown ≈ 24 km/h vs 35 km/h elsewhere |
| Segments | 6,256 stop pairs (1,841 with ≥15 traversals); median delay added −8 s: delay builds in a few hot spots and is slowly recovered elsewhere |
| Equity (more data) | Tracts ≥20% car-free 66.8% on time vs 66.5% for <10%: **no gap** (the Phase 3 difference was noise from ~1 day of data) |

## Updated 60-second pitch (add)

> "...I didn't trust one feed's definition of an arrival, so I validated it against the other: a Spark range join matches every reported arrival to the bus's GPS track and finds the closest fix to the stop. It showed the feed's times run ahead of the bus, so 'early' is overstated and 'late' understated. I added H3 hexagons in PostGIS to map moving speed, a stop-to-stop model of where buses lose time, and a read-only role for Power BI. CI builds the same Postgres image with H3, tests every model against fixtures with known answers, and publishes a public MapLibre map and the dbt docs to GitHub Pages."
