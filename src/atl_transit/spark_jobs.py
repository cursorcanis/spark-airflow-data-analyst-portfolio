"""PySpark curation jobs: raw zone -> curated Delta tables.

Split in two layers on purpose:
  * prepare_* functions are pure DataFrame -> DataFrame transformations (unit-tested with a local
    SparkSession, no lake and no Delta needed).
  * write_* / curate_* functions do the I/O: read the lake via s3a, MERGE into Delta tables.
Only DataFrame and SQL APIs are used, so the same code runs on a classic SparkSession, through
Spark Connect (how Airflow calls it), or on Databricks.
Python 3.10-compatible: it also runs inside the Spark image.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

LOCAL_TZ = "America/New_York"
# Generous bounding box around the 11-county Atlanta region; GPS points outside are rejected.
ATL_BBOX = {"lat_min": 33.0, "lat_max": 34.7, "lon_min": -85.3, "lon_max": -83.5}

# ------------------------------------------------------------------ GTFS static
# Typed column spec per GTFS table. Columns missing from a feed are added as typed NULLs,
# so the curated schema is stable even if MARTA drops or adds optional columns.
STATIC_SPEC: dict[str, dict[str, str]] = {
    "agency": {"agency_id": "string", "agency_name": "string", "agency_url": "string", "agency_timezone": "string"},
    "routes": {"route_id": "string", "agency_id": "string", "route_short_name": "string",
               "route_long_name": "string", "route_type": "int", "route_color": "string",
               "route_text_color": "string"},
    "stops": {"stop_id": "string", "stop_code": "string", "stop_name": "string", "stop_desc": "string",
              "stop_lat": "double", "stop_lon": "double", "location_type": "int", "parent_station": "string",
              "wheelchair_boarding": "int"},
    "trips": {"route_id": "string", "service_id": "string", "trip_id": "string", "trip_headsign": "string",
              "direction_id": "int", "block_id": "string", "shape_id": "string", "wheelchair_accessible": "int"},
    "stop_times": {"trip_id": "string", "arrival_time": "string", "departure_time": "string",
                   "stop_id": "string", "stop_sequence": "int", "pickup_type": "int", "drop_off_type": "int",
                   "shape_dist_traveled": "double", "timepoint": "int"},
    "calendar": {"service_id": "string", "monday": "int", "tuesday": "int", "wednesday": "int",
                 "thursday": "int", "friday": "int", "saturday": "int", "sunday": "int",
                 "start_date": "string", "end_date": "string"},
    "calendar_dates": {"service_id": "string", "date": "string", "exception_type": "int"},
    "shapes": {"shape_id": "string", "shape_pt_lat": "double", "shape_pt_lon": "double",
               "shape_pt_sequence": "int", "shape_dist_traveled": "double"},
}
DATE_COLUMNS = {"calendar": ["start_date", "end_date"], "calendar_dates": ["date"]}


def gtfs_time_to_seconds(col):
    """'HH:MM:SS' -> seconds after service-day start. GTFS allows hours >= 24 (trips past midnight)."""
    parts = F.split(F.trim(col), ":")
    return (parts.getItem(0).cast("int") * 3600 + parts.getItem(1).cast("int") * 60
            + parts.getItem(2).cast("int"))


def prepare_static_table(table: str, df: DataFrame, feed_ingest_date: str) -> DataFrame:
    spec = STATIC_SPEC[table]
    cols = [
        (F.trim(F.col(name)).cast(dtype) if name in df.columns else F.lit(None).cast(dtype)).alias(name)
        for name, dtype in spec.items()
    ]
    out = df.select(*cols)
    for c in DATE_COLUMNS.get(table, []):
        out = out.withColumn(c, F.to_date(F.col(c), "yyyyMMdd"))
    if table == "stop_times":
        out = (out.withColumn("arrival_secs", gtfs_time_to_seconds(F.col("arrival_time")))
                  .withColumn("departure_secs", gtfs_time_to_seconds(F.col("departure_time"))))
    return out.withColumn("feed_ingest_date", F.to_date(F.lit(feed_ingest_date)))


def curate_gtfs_static(spark: SparkSession, raw_tables_path: str, curated_base: str,
                       feed_ingest_date: str) -> dict[str, int]:
    """Full refresh of every curated GTFS static table (small, versioned by Delta time travel)."""
    counts = {}
    for table in STATIC_SPEC:
        raw = spark.read.option("header", True).csv(f"{raw_tables_path.rstrip('/')}/{table}.txt")
        df = prepare_static_table(table, raw, feed_ingest_date)
        (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
           .save(f"{curated_base}/gtfs_static/{table}"))
        counts[table] = spark.read.format("delta").load(f"{curated_base}/gtfs_static/{table}").count()
    return counts


# ------------------------------------------------------------------ vehicle positions
VP_COLUMNS = ["event_date", "event_ts", "vehicle_ts", "vehicle_id", "vehicle_label", "trip_id", "route_id",
              "direction_id", "latitude", "longitude", "bearing", "speed_mps", "feed_ts"]


def prepare_vehicle_positions(raw: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Returns (clean, rejected). One row per (vehicle_id, vehicle_ts): a vehicle that hasn't sent a new
    GPS fix is repeated in every 30-second snapshot, so duplicates are expected and removed."""
    in_bbox = (F.col("latitude").between(ATL_BBOX["lat_min"], ATL_BBOX["lat_max"])
               & F.col("longitude").between(ATL_BBOX["lon_min"], ATL_BBOX["lon_max"]))
    valid = (F.col("vehicle_id").isNotNull() & F.col("vehicle_ts").isNotNull()
             & F.col("latitude").isNotNull() & F.col("longitude").isNotNull() & in_bbox)
    flagged = raw.withColumn("_valid", F.coalesce(valid, F.lit(False)))
    rejected = flagged.filter(~F.col("_valid")).drop("_valid")

    w = Window.partitionBy("vehicle_id", "vehicle_ts").orderBy(F.col("feed_ts").asc())
    clean = (flagged.filter("_valid").drop("_valid")
             .withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")   # earliest sighting wins
             .withColumn("event_ts", F.timestamp_seconds("vehicle_ts"))
             .withColumn("event_date", F.to_date(F.from_utc_timestamp("event_ts", LOCAL_TZ)))
             .select(*VP_COLUMNS))
    return clean, rejected


# ------------------------------------------------------------------ stop arrivals
ARRIVAL_COLUMNS = ["service_date", "trip_id", "stop_sequence", "route_id", "stop_id", "rt_stop_id", "vehicle_id",
                   "observed_arrival_ts", "scheduled_arrival_ts", "delay_s", "has_schedule", "feed_ts"]


def prepare_stop_arrivals(trip_updates: DataFrame, stop_times: DataFrame) -> DataFrame:
    """Observed arrival per (service_date, trip, stop_sequence) joined to the schedule.

    MARTA's trip updates keep reporting stops the vehicle has already passed. A row whose
    arrival_time <= the snapshot's feed_ts describes a stop already served; the latest such report
    is taken as the observed arrival. The live feed's stop_id is an internal code that often isn't in
    the published schedule, so we join on (trip_id, stop_sequence), which matches 100%.
    """
    passed = trip_updates.filter(F.col("arrival_time").isNotNull() & (F.col("arrival_time") <= F.col("feed_ts")))
    w = Window.partitionBy("start_date", "trip_id", "stop_sequence").orderBy(F.col("feed_ts").desc())
    latest = (passed.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
              .withColumn("service_date", F.to_date("start_date", "yyyyMMdd")))

    sched = stop_times.select("trip_id", "stop_sequence", F.col("stop_id").alias("sched_stop_id"), "arrival_secs")
    joined = latest.join(sched, ["trip_id", "stop_sequence"], "left")

    # GTFS times count from "noon minus 12 h" on the service date in local time (correct across DST changes).
    noon_utc = F.to_utc_timestamp(
        F.to_timestamp(F.concat(F.date_format("service_date", "yyyy-MM-dd"), F.lit(" 12:00:00"))), LOCAL_TZ)
    sched_epoch = F.unix_timestamp(noon_utc) - 43200 + F.col("arrival_secs")

    return (joined
            .withColumn("scheduled_arrival_ts", F.timestamp_seconds(sched_epoch))
            .withColumn("observed_arrival_ts", F.timestamp_seconds("arrival_time"))
            .withColumn("delay_s", (F.col("arrival_time") - sched_epoch).cast("int"))
            .withColumn("has_schedule", F.col("arrival_secs").isNotNull())
            .withColumnRenamed("stop_id", "rt_stop_id")
            .withColumn("stop_id", F.coalesce("sched_stop_id", "rt_stop_id"))
            .select(*ARRIVAL_COLUMNS))


# ------------------------------------------------------------------ GPS-derived tables (Phase 4)
EARTH_RADIUS_M = 6371000.0


def haversine_m(lat1, lon1, lat2, lon2):
    """Great-circle distance in metres between two lat/lon columns (pure column expression, no UDF)."""
    dlat = F.radians(lat2 - lat1)
    dlon = F.radians(lon2 - lon1)
    a = F.sin(dlat / 2) ** 2 + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(dlon / 2) ** 2
    return F.lit(2 * EARTH_RADIUS_M) * F.asin(F.sqrt(a))


PING_COLUMNS = ["event_date", "event_ts", "vehicle_ts", "vehicle_id", "trip_id", "route_id", "direction_id",
                "latitude", "longitude", "reported_speed_mps", "calc_speed_mps", "secs_since_prev"]
MAX_BUS_SPEED_MPS = 35.0     # ~126 km/h: faster than any bus, so a GPS jump


def prepare_vehicle_pings(vp: DataFrame) -> DataFrame:
    """One row per GPS fix with the speed since the vehicle's previous fix on the same trip.

    calc_speed_mps = distance / time between consecutive fixes. It is NULL when there's no usable previous
    fix (first fix of a trip, a gap > 5 min, or an impossible jump), so averages aren't skewed.
    """
    w = Window.partitionBy("vehicle_id").orderBy("vehicle_ts")
    prev_lat, prev_lon = F.lag("latitude").over(w), F.lag("longitude").over(w)
    dt = F.col("vehicle_ts") - F.lag("vehicle_ts").over(w)
    same_trip = F.lag("trip_id").over(w) == F.col("trip_id")
    out = (vp.withColumn("secs_since_prev", dt.cast("int"))
             .withColumn("_dist", haversine_m(prev_lat, prev_lon, F.col("latitude"), F.col("longitude")))
             .withColumn("_speed", F.col("_dist") / F.col("secs_since_prev")))
    usable = same_trip & F.col("secs_since_prev").between(5, 300) & (F.col("_speed") <= MAX_BUS_SPEED_MPS)
    return (out.withColumn("calc_speed_mps", F.when(usable, F.round("_speed", 2)))
               .withColumn("reported_speed_mps", F.col("speed_mps").cast("double"))
               .select(*PING_COLUMNS))


GPS_CHECK_COLUMNS = ["service_date", "trip_id", "stop_sequence", "stop_id", "observed_arrival_ts",
                     "gps_pings_in_window", "gps_min_dist_m", "gps_closest_ts", "gps_offset_s", "gps_confirmed"]


def prepare_arrival_gps_check(arrivals: DataFrame, pings: DataFrame, stops: DataFrame,
                              window_s: int = 1200, confirm_m: float = 150.0) -> DataFrame:
    """Checks each feed-reported arrival against the bus's own GPS track.

    For every arrival, look at that trip's GPS fixes within +/- window_s of the reported time and find the
    fix closest to the stop. gps_offset_s = reported arrival - time of that closest fix (positive = the feed
    says the bus came later than GPS shows). gps_confirmed = the bus really came within confirm_m of the stop.
    Fixes are ~30 s apart, so the closest fix is within ~15 s of the true pass time.
    """
    a = (arrivals.filter("has_schedule")
         .join(stops.select("stop_id", "stop_lat", "stop_lon"), "stop_id")
         .select("service_date", "trip_id", "stop_sequence", "stop_id", "observed_arrival_ts",
                 F.unix_timestamp("observed_arrival_ts").alias("obs_epoch"), "stop_lat", "stop_lon")
         .alias("a"))
    p = pings.select(F.col("trip_id").alias("p_trip_id"), F.col("vehicle_ts").alias("ping_epoch"),
                     F.col("latitude").alias("p_lat"), F.col("longitude").alias("p_lon")).alias("p")
    cond = ((F.col("a.trip_id") == F.col("p.p_trip_id"))
            & F.col("p.ping_epoch").between(F.col("a.obs_epoch") - window_s, F.col("a.obs_epoch") + window_s))
    keys = ["service_date", "trip_id", "stop_sequence", "stop_id", "observed_arrival_ts", "obs_epoch"]
    agg = (a.join(p, cond, "left")
           .withColumn("_dist", haversine_m(F.col("stop_lat"), F.col("stop_lon"), F.col("p_lat"), F.col("p_lon")))
           .groupBy(*keys)
           .agg(F.count("ping_epoch").alias("gps_pings_in_window"),
                F.min("_dist").alias("_min_dist"),
                F.min_by("ping_epoch", "_dist").alias("_closest_epoch")))
    return (agg.withColumn("gps_min_dist_m", F.round("_min_dist", 1))
               .withColumn("gps_closest_ts", F.timestamp_seconds("_closest_epoch"))
               .withColumn("gps_offset_s", (F.col("obs_epoch") - F.col("_closest_epoch")).cast("int"))
               .withColumn("gps_confirmed", F.coalesce(F.col("_min_dist") <= confirm_m, F.lit(False)))
               .select(*GPS_CHECK_COLUMNS))


# ------------------------------------------------------------------ Delta writers (MERGE = idempotent upserts)
VP_DDL = """CREATE TABLE IF NOT EXISTS delta.`{path}` (
  event_date date, event_ts timestamp, vehicle_ts bigint, vehicle_id string, vehicle_label string,
  trip_id string, route_id string, direction_id int, latitude double, longitude double,
  bearing float, speed_mps float, feed_ts bigint) USING delta PARTITIONED BY (event_date)"""

ARRIVALS_DDL = """CREATE TABLE IF NOT EXISTS delta.`{path}` (
  service_date date, trip_id string, stop_sequence int, route_id string, stop_id string, rt_stop_id string,
  vehicle_id string, observed_arrival_ts timestamp, scheduled_arrival_ts timestamp, delay_s int,
  has_schedule boolean, feed_ts bigint) USING delta PARTITIONED BY (service_date)"""


def _date_list_literal(df: DataFrame, col: str) -> str:
    dates = sorted(r[0].isoformat() for r in df.select(col).distinct().collect() if r[0] is not None)
    return ", ".join(f"DATE'{d}'" for d in dates)


def merge_vehicle_positions(spark: SparkSession, clean: DataFrame, path: str) -> None:
    spark.sql(VP_DDL.format(path=path))
    dates = _date_list_literal(clean, "event_date")
    if not dates:
        return
    clean.createOrReplaceTempView("vp_src")
    # The partition filter on t.event_date lets Delta skip every other day's files (partition pruning).
    spark.sql(f"""
        MERGE INTO delta.`{path}` t USING vp_src s
        ON t.event_date IN ({dates}) AND t.event_date = s.event_date
           AND t.vehicle_id = s.vehicle_id AND t.vehicle_ts = s.vehicle_ts
        WHEN NOT MATCHED THEN INSERT *""")


def merge_stop_arrivals(spark: SparkSession, arrivals: DataFrame, path: str) -> None:
    spark.sql(ARRIVALS_DDL.format(path=path))
    dates = _date_list_literal(arrivals, "service_date")
    if not dates:
        return
    arrivals.createOrReplaceTempView("arrivals_src")
    # Newer reports win regardless of processing order -> re-running or backfilling hours is safe.
    spark.sql(f"""
        MERGE INTO delta.`{path}` t USING arrivals_src s
        ON t.service_date IN ({dates}) AND t.service_date = s.service_date
           AND t.trip_id = s.trip_id AND t.stop_sequence = s.stop_sequence
        WHEN MATCHED AND s.feed_ts > t.feed_ts THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *""")


def curate_rt_hour(spark: SparkSession, raw_base: str, curated_base: str, date: str, hour: str) -> dict:
    """Curate one compacted hour of realtime data into the vehicle_positions and stop_arrivals tables."""
    part = f"date={date}/hour={hour}"
    stats = {"hour": part}

    vp_raw = spark.read.parquet(f"{raw_base}/gtfs_rt_parquet/feed=vehicle_positions/{part}/")
    vp_clean, vp_rejected = prepare_vehicle_positions(vp_raw)
    stats["vp_raw"], stats["vp_clean"], stats["vp_rejected"] = vp_raw.count(), vp_clean.count(), vp_rejected.count()
    merge_vehicle_positions(spark, vp_clean, f"{curated_base}/vehicle_positions")

    tu = spark.read.parquet(f"{raw_base}/gtfs_rt_parquet/feed=trip_updates/{part}/")
    stop_times = spark.read.format("delta").load(f"{curated_base}/gtfs_static/stop_times")
    arrivals = prepare_stop_arrivals(tu, stop_times)
    stats["arrivals"] = arrivals.count()
    stats["arrivals_unscheduled"] = arrivals.filter(~F.col("has_schedule")).count()
    merge_stop_arrivals(spark, arrivals, f"{curated_base}/stop_arrivals")
    return stats


# ------------------------------------------------------------------ export to the Postgres warehouse
# Spark executors write each table into a *load* table (raw._load_<name>, dropped and recreated).
# Airflow then publishes it into the real table inside ONE Postgres transaction (see warehouse_sql),
# so dbt and BI users never see a half-loaded table.
WAREHOUSE_STATIC_TABLES = ["routes", "stops", "trips", "stop_times", "calendar", "calendar_dates", "shapes"]


def write_load_table(df: DataFrame, load_table: str, jdbc: dict) -> None:
    (df.write.format("jdbc")
       .option("url", jdbc["url"]).option("user", jdbc["user"]).option("password", jdbc["password"])
       .option("driver", "org.postgresql.Driver")
       .option("dbtable", load_table)
       .option("batchsize", "10000")      # rows per INSERT batch
       .option("numPartitions", "4")      # max parallel JDBC connections
       .mode("overwrite").save())


def export_gtfs_static(spark: SparkSession, curated_base: str, jdbc: dict) -> dict[str, int]:
    counts = {}
    for table in WAREHOUSE_STATIC_TABLES:
        df = spark.read.format("delta").load(f"{curated_base}/gtfs_static/{table}")
        write_load_table(df, f"raw._load_gtfs_{table}", jdbc)
        counts[table] = df.count()
    return counts


def export_stop_arrivals_window(spark: SparkSession, curated_base: str, jdbc: dict, cutoff_date: str) -> int:
    """Lookback window: re-export every service_date >= cutoff (late MERGE updates land in recent days)."""
    df = (spark.read.format("delta").load(f"{curated_base}/stop_arrivals")
          .filter(F.col("service_date") >= F.to_date(F.lit(cutoff_date))))
    write_load_table(df, "raw._load_rt_stop_arrivals", jdbc)
    return df.count()


def export_rt_gps_window(spark: SparkSession, curated_base: str, jdbc: dict, cutoff_date: str) -> dict[str, int]:
    """GPS pings (with speeds) and the arrival-vs-GPS check for every date >= cutoff, into two load tables."""
    cutoff = F.to_date(F.lit(cutoff_date))
    vp = spark.read.format("delta").load(f"{curated_base}/vehicle_positions").filter(F.col("event_date") >= cutoff)
    arrivals = (spark.read.format("delta").load(f"{curated_base}/stop_arrivals")
                .filter(F.col("service_date") >= cutoff))
    stops = spark.read.format("delta").load(f"{curated_base}/gtfs_static/stops")

    pings = prepare_vehicle_pings(vp)
    write_load_table(pings, "raw._load_rt_vehicle_pings", jdbc)
    check = prepare_arrival_gps_check(arrivals, vp, stops)
    write_load_table(check, "raw._load_rt_arrival_gps_check", jdbc)
    return {"pings": pings.count(), "gps_checks": check.count()}
