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
