"""Export dbt marts as small GeoJSON/JSON files for the static web map (web/ on GitHub Pages).

PostGIS builds the GeoJSON itself (ST_AsGeoJSON + json_build_object), so Python only writes strings to disk.
Coordinates are rounded to 5 decimals (~1 m) and tract polygons simplified, keeping the files small.

Usage (reads the standard libpq env vars PGHOST, PGUSER, PGPASSWORD, PGDATABASE):
    python -m atl_transit.webmap OUT_DIR
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

# Minimum sample sizes before a hexagon/segment is shown: a map of 3-observation averages is noise.
MIN_SPEED_SAMPLES = 30
MIN_SEGMENT_TRAVERSALS = 15
MIN_TRACT_ARRIVALS = 50


def _feature_collection(select_sql: str) -> str:
    """Wrap a query returning (geom, props jsonb) rows into one FeatureCollection document."""
    return f"""
        select json_build_object(
            'type', 'FeatureCollection',
            'features', coalesce(json_agg(json_build_object(
                'type', 'Feature',
                'geometry', st_asgeojson(q.geom, 5)::json,
                'properties', q.props)), '[]'::json))::text
        from ({select_sql}) as q"""


LAYERS: dict[str, str] = {
    "h3_bus_speed.geojson": _feature_collection(f"""
        select geom, jsonb_build_object(
            'h3', h3_cell, 'speed_kmh', median_moving_speed_kmh, 'peak_speed_kmh', peak_median_moving_speed_kmh,
            'pct_stopped', pct_stopped, 'samples', speed_samples, 'routes', routes,
            'stops', stops_observed, 'arrivals', arrivals_observed, 'pct_on_time', pct_on_time) as props
        from marts.mart_h3_bus_speed
        where speed_samples >= {MIN_SPEED_SAMPLES} and median_moving_speed_kmh is not null"""),
    "segment_delay.geojson": _feature_collection(f"""
        select geom, jsonb_build_object(
            'from', from_stop_name, 'to', to_stop_name, 'routes', routes, 'n', traversals,
            'avg_added_s', avg_delay_added_s, 'median_added_s', median_delay_added_s, 'length_m', length_m) as props
        from marts.mart_segment_delay
        where traversals >= {MIN_SEGMENT_TRAVERSALS} and length_m between 20 and 5000"""),
    "tract_equity.geojson": _feature_collection(f"""
        select st_simplifypreservetopology(geom, 0.0005) as geom, jsonb_build_object(
            'geoid', geoid, 'county', county_name, 'population', total_population,
            'income', median_household_income, 'pct_no_car', pct_households_no_vehicle,
            'pct_transit', pct_transit_commute, 'poverty', poverty_rate, 'stops', stops_observed,
            'arrivals', arrivals_observed,
            'pct_on_time', case when arrivals_observed >= {MIN_TRACT_ARRIVALS} then pct_on_time end) as props
        from marts.mart_tract_service_equity"""),
}

SUMMARY_SQL = """
with f as (select * from marts.fct_stop_arrival),
gps as (
    select
        round(100.0 * sum(arrivals) filter (where feed_class = 'early' and gps_class = 'on_time')
              / nullif(sum(arrivals) filter (where feed_class = 'early' and gps_class <> 'not_confirmed'), 0), 1)
            as pct_feed_early_actually_on_time,
        round(100.0 * sum(arrivals) filter (where gps_class = 'on_time')
              / nullif(sum(arrivals) filter (where gps_class <> 'not_confirmed'), 0), 1) as pct_on_time_gps,
        round(100.0 * sum(arrivals) filter (where gps_class <> 'not_confirmed') / nullif(sum(arrivals), 0), 1)
            as pct_gps_confirmed,
        -- early/late shares on the GPS-confirmed subset, by the feed's clock and by the GPS clock
        round(100.0 * sum(arrivals) filter (where gps_class <> 'not_confirmed' and feed_class = 'early')
              / nullif(sum(arrivals) filter (where gps_class <> 'not_confirmed'), 0), 1) as pct_early_feed,
        round(100.0 * sum(arrivals) filter (where gps_class = 'early')
              / nullif(sum(arrivals) filter (where gps_class <> 'not_confirmed'), 0), 1) as pct_early_gps,
        round(100.0 * sum(arrivals) filter (where gps_class <> 'not_confirmed' and feed_class = 'late')
              / nullif(sum(arrivals) filter (where gps_class <> 'not_confirmed'), 0), 1) as pct_late_feed,
        round(100.0 * sum(arrivals) filter (where gps_class = 'late')
              / nullif(sum(arrivals) filter (where gps_class <> 'not_confirmed'), 0), 1) as pct_late_gps
    from marts.mart_gps_validation
),
worst as (
    select json_agg(w order by w.pct_on_time) as worst_routes from (
        select route_short_name as route, sum(arrivals) as arrivals,
               round(sum(arrivals * pct_on_time) / sum(arrivals), 1) as pct_on_time
        from marts.mart_route_on_time_daily
        group by 1 having sum(arrivals) >= 300
        order by 3 limit 5) as w
)
select json_build_object(
    'first_service_date', (select min(service_date) from f),
    'last_service_date', (select max(service_date) from f),
    'arrivals', (select count(*) from f),
    'routes_observed', (select count(distinct route_id) from f),
    'pct_on_time', (select round(100.0 * avg(is_on_time::int), 1) from f),
    'pct_early', (select round(100.0 * avg(is_early::int), 1) from f),
    'pct_late', (select round(100.0 * avg(is_late::int), 1) from f),
    'gps_fixes', (select count(*) from marts.fct_vehicle_ping),
    'gps', (select row_to_json(gps) from gps),
    'worst_routes', (select worst_routes from worst),
    'exported_at', now()
)::text"""


def export_webmap(conn, out_dir: str | Path) -> dict[str, int]:
    """Writes every layer + summary.json into out_dir; returns bytes written per file."""
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    sizes = {}
    with conn.cursor() as cur:
        for name, sql in {**LAYERS, "summary.json": SUMMARY_SQL}.items():
            cur.execute(sql)
            text = cur.fetchone()[0]
            json.loads(text)                     # fail loudly on anything that isn't valid JSON
            (out / name).write_text(text, encoding="utf-8")
            sizes[name] = len(text.encode("utf-8"))
    return sizes


def main(argv: list[str]) -> int:
    import psycopg2

    if len(argv) != 2:
        print(__doc__, file=sys.stderr)
        return 2
    conn = psycopg2.connect("")                  # empty DSN: libpq reads PG* environment variables
    try:
        conn.set_session(readonly=True)
        for name, size in export_webmap(conn, argv[1]).items():
            print(f"{name}: {size / 1024:.0f} KiB", file=sys.stderr)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
