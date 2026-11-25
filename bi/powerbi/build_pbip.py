"""Generate the Power BI report as a Power BI Project (.pbip): model + pages as text, so it lives in git.

    python bi/powerbi/build_pbip.py --server <host:port> --out bi/powerbi/local     # your working copy
    python bi/powerbi/build_pbip.py                                                 # repo copy, placeholder host

Then open "<out>/ATL Transit.pbip" in Power BI Desktop, enter the `reporting` password once
(Database credentials) and click Refresh. The server address is a Power Query parameter (PgServer):
Transform data > Edit parameters to change it.

What it builds:
  * Semantic model (model.bim, TMSL): the 10 marts tables in Import mode (geometry columns left out),
    4 star-schema relationships, dim_date marked as the date table, every measure from measures.dax.
  * Report (report.json): 5 pages: Overview, Time of day, Stops map, Equity, GPS check.
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
from pathlib import Path

HERE = Path(__file__).resolve().parent
NAME = "ATL Transit"

# table -> [(column, postgres type)]; geometry columns are deliberately absent.
TABLES: dict[str, list[tuple[str, str]]] = {
    "dim_date": [("date_day", "date"), ("iso_day_of_week", "int"), ("day_name", "text"), ("is_weekend", "bool"),
                 ("month", "int"), ("year", "int")],
    "dim_route": [("route_id", "text"), ("route_short_name", "text"), ("route_long_name", "text"),
                  ("route_mode", "text"), ("route_color", "text"), ("scheduled_trips_in_feed", "int")],
    "dim_stop": [("stop_id", "text"), ("stop_code", "text"), ("stop_name", "text"), ("stop_lat", "float"),
                 ("stop_lon", "float"), ("is_rail_station_area", "bool"), ("is_wheelchair_accessible", "bool"),
                 ("tract_geoid", "text"), ("county_name", "text"), ("h3_cell", "text")],
    "dim_tract": [("geoid", "text"), ("county_name", "text"), ("tract_name", "text"), ("land_km2", "float"),
                  ("acs_year", "int"), ("total_population", "int"), ("population_per_km2", "float"),
                  ("median_household_income", "int"), ("households_no_vehicle", "int"),
                  ("pct_households_no_vehicle", "float"), ("pct_transit_commute", "float"),
                  ("poverty_rate", "float")],
    "fct_stop_arrival": [("service_date", "date"), ("trip_id", "text"), ("stop_sequence", "int"),
                         ("route_id", "text"), ("stop_id", "text"), ("direction_id", "int"), ("vehicle_id", "text"),
                         ("scheduled_arrival_at", "timestamp"), ("observed_arrival_at", "timestamp"),
                         ("delay_s", "int"), ("delay_min", "float"), ("scheduled_hour_local", "int"),
                         ("is_on_time", "bool"), ("is_late", "bool"), ("is_early", "bool")],
    "mart_route_on_time_daily": [("service_date", "date"), ("route_id", "text"), ("route_short_name", "text"),
                                 ("route_long_name", "text"), ("route_mode", "text"), ("arrivals", "int"),
                                 ("pct_on_time", "float"), ("pct_late", "float"), ("pct_early", "float"),
                                 ("median_delay_min", "float"), ("p90_delay_min", "float")],
    "mart_on_time_by_hour": [("scheduled_hour_local", "int"), ("is_weekend", "bool"), ("arrivals", "int"),
                             ("pct_on_time", "float"), ("median_delay_min", "float")],
    "mart_tract_service_equity": [("geoid", "text"), ("county_name", "text"), ("total_population", "int"),
                                  ("median_household_income", "int"), ("pct_households_no_vehicle", "float"),
                                  ("pct_transit_commute", "float"), ("poverty_rate", "float"),
                                  ("stops_observed", "int"), ("arrivals_observed", "int"), ("pct_on_time", "float"),
                                  ("median_delay_min", "float")],
    "mart_gps_validation": [("feed_class", "text"), ("gps_class", "text"), ("arrivals", "int"),
                            ("pct_of_feed_class", "float"), ("median_gps_offset_s", "float"),
                            ("median_gps_dist_m", "float")],
    "mart_h3_bus_speed": [("h3_cell", "text"), ("gps_fixes", "int"), ("routes", "int"), ("speed_samples", "int"),
                          ("pct_stopped", "float"), ("median_moving_speed_kmh", "float"),
                          ("peak_median_moving_speed_kmh", "float"), ("stops_observed", "int"),
                          ("arrivals_observed", "int"), ("pct_on_time", "float")],
}
DATA_TYPES = {"text": "string", "int": "int64", "float": "double", "bool": "boolean",
              "date": "dateTime", "timestamp": "dateTime"}
KEYS = {"route_id", "stop_id", "trip_id", "geoid", "tract_geoid", "h3_cell", "vehicle_id", "stop_code",
        "direction_id", "stop_sequence", "iso_day_of_week", "month", "year", "acs_year", "scheduled_hour_local"}
RELATIONSHIPS = [  # (many table, column, one table, column)
    ("fct_stop_arrival", "route_id", "dim_route", "route_id"),
    ("fct_stop_arrival", "stop_id", "dim_stop", "stop_id"),
    ("fct_stop_arrival", "service_date", "dim_date", "date_day"),
    ("dim_stop", "tract_geoid", "dim_tract", "geoid"),
]
# Extra measures used by the pages (the rest come from measures.dax).
EXTRA_MEASURES = {
    "On-time % (routes 300+)": "IF ( [Arrivals] >= 300, [On-time %] )",
    "On-time % (stops 50+)": "IF ( [Arrivals] >= 50, [On-time %] )",
    # sorted descending, so stops with too few arrivals (blank) fall to the bottom instead of the top
    "Not on time % (stops 50+)": "IF ( [Arrivals] >= 50, 1 - [On-time %] )",
}
PERCENT = "0.0%;-0.0%;0.0%"


# ------------------------------------------------------------------ semantic model
def parse_measures(dax: str) -> dict[str, str]:
    """measures.dax blocks: 'Name =' then the expression, separated by blank lines; // comments ignored."""
    measures = {}
    for block in re.split(r"\n\s*\n", dax):
        lines = [ln for ln in block.strip().splitlines() if not ln.lstrip().startswith("//")]
        if not lines:
            continue
        m = re.match(r"^(.+?)\s*=\s*(.*)$", lines[0])
        if not m:
            raise ValueError(f"cannot parse measure block: {block[:60]!r}")
        measures[m.group(1).strip()] = "\n".join([m.group(2), *lines[1:]]).strip()
    return measures


def m_query(table: str, cols: list[tuple[str, str]]) -> list[str]:
    col_list = ", ".join(f'"{c}"' for c, _ in cols)
    steps = [
        "let",
        '    Source = PostgreSQL.Database(PgServer, "warehouse"),',
        f'    Data = Source{{[Schema = "marts", Item = "{table}"]}}[Data],',
        f"    Kept = Table.SelectColumns(Data, {{{col_list}}})",
    ]
    if table == "mart_tract_service_equity":  # the equity scatter only makes sense with enough observations
        steps[-1] += ","
        steps.append("    Enough = Table.SelectRows(Kept, each [arrivals_observed] >= 50)")
        steps += ["in", "    Enough"]
    else:
        steps += ["in", "    Kept"]
    return steps


def column(name: str, pg_type: str) -> dict:
    col = {"name": name, "dataType": DATA_TYPES[pg_type], "sourceColumn": name,
           "summarizeBy": "none" if name in KEYS or pg_type not in ("int", "float") else "sum"}
    if pg_type == "date":
        col["formatString"] = "yyyy-mm-dd"
    elif pg_type == "timestamp":
        col["formatString"] = "yyyy-mm-dd hh:nn"
    elif pg_type == "float":
        col["formatString"] = "0.0"
    if name in ("stop_lat",):
        col["dataCategory"] = "Latitude"
    if name in ("stop_lon",):
        col["dataCategory"] = "Longitude"
    return col


def semantic_model(server: str, measures: dict[str, str]) -> dict:
    tables = []
    for name, cols in TABLES.items():
        t = {"name": name, "columns": [column(c, ty) for c, ty in cols],
             "partitions": [{"name": name, "mode": "import", "source": {"type": "m", "expression": m_query(name, cols)}}]}
        if name == "dim_date":
            t["dataCategory"] = "Time"
            for c in t["columns"]:
                if c["name"] == "date_day":
                    c["isKey"] = True
        tables.append(t)
    tables.append({
        "name": "_Measures",
        "columns": [{"name": "Measures", "dataType": "string", "sourceColumn": "Measures", "isHidden": True}],
        "partitions": [{"name": "_Measures", "mode": "import",
                        "source": {"type": "m", "expression": ["let", '    Source = #table(type table [Measures = text], {})',
                                                              "in", "    Source"]}}],
        "measures": [
            {"name": n, "expression": e,
             "formatString": PERCENT if "%" in n else ("0.0" if "(min)" in n else "#,0")}
            for n, e in {**measures, **EXTRA_MEASURES}.items()
        ],
    })
    return {
        "name": NAME,
        "compatibilityLevel": 1567,
        "model": {
            "culture": "en-US",
            "defaultPowerBIDataSourceVersion": "powerBI_V3",
            "sourceQueryCulture": "en-US",
            "dataAccessOptions": {"legacyRedirects": True, "returnErrorValuesAsNull": True},
            "tables": tables,
            "relationships": [
                {"name": f"{a}_{ac}__{b}_{bc}", "fromTable": a, "fromColumn": ac, "toTable": b, "toColumn": bc}
                for a, ac, b, bc in RELATIONSHIPS
            ],
            "expressions": [{
                "name": "PgServer", "kind": "m",
                "expression": f'"{server}" meta [IsParameterQuery=true, Type="Text", IsParameterQueryRequired=true]',
                "annotations": [{"name": "PBI_ResultType", "value": "Text"}],
            }],
            "annotations": [{"name": "PBI_QueryOrder", "value": json.dumps(["PgServer", *TABLES, "_Measures"])},
                            {"name": "PBIDesktopVersion", "value": "2.160"}],
        },
    }


# ------------------------------------------------------------------ report (legacy report.json layout)
class Q:
    """Builds the prototypeQuery + projections for one visual."""

    def __init__(self):
        self.sources: dict[str, str] = {}
        self.select: list[dict] = []
        self.order: list[dict] = []
        self.roles: dict[str, list[dict]] = {}

    def _src(self, entity: str) -> dict:
        alias = self.sources.setdefault(entity, f"t{len(self.sources)}")
        return {"SourceRef": {"Source": alias}}

    def measure(self, role: str, name: str, entity: str = "_Measures", sort: str | None = None) -> Q:
        expr = {"Measure": {"Expression": self._src(entity), "Property": name}}
        return self._add(role, expr, f"{entity}.{name}", sort)

    def col(self, role: str, entity: str, name: str, agg: int | None = None, sort: str | None = None) -> Q:
        expr = {"Column": {"Expression": self._src(entity), "Property": name}}
        ref = f"{entity}.{name}"
        if agg is not None:  # 0 Sum, 1 Avg, 3 Min, 4 Max, 5 CountNonNull
            expr = {"Aggregation": {"Expression": expr, "Function": agg}}
            ref = f"{['Sum', 'Avg', 'Count', 'Min', 'Max', 'CountNonNull'][agg]}({entity}.{name})"
        return self._add(role, expr, ref, sort)

    def _add(self, role, expr, ref, sort) -> Q:
        self.select.append({**expr, "Name": ref})
        proj = {"queryRef": ref}
        if role in ("Category", "Rows", "Columns") and not any(p.get("active") for p in self.roles.get(role, [])):
            proj["active"] = True
        self.roles.setdefault(role, []).append(proj)
        if sort:
            self.order.append({"Direction": 1 if sort == "asc" else 2, "Expression": expr})
        return self

    def build(self) -> tuple[dict, dict]:
        query = {"Version": 2, "From": [{"Name": a, "Entity": e, "Type": 0} for e, a in self.sources.items()],
                 "Select": self.select}
        if self.order:
            query["OrderBy"] = self.order
        return self.roles, query


def lit(value) -> dict:
    v = f"'{value}'" if isinstance(value, str) else ("true" if value is True else "false" if value is False else f"{value}D")
    return {"expr": {"Literal": {"Value": v}}}


def visual(vid: str, vtype: str, pos: tuple[int, int, int, int], q: Q | None = None, title: str | None = None,
           objects: dict | None = None, z: int = 0) -> dict:
    x, y, w, h = pos
    single: dict = {"visualType": vtype}
    if q is not None:
        roles, query = q.build()
        single["projections"], single["prototypeQuery"] = roles, query
    if objects:
        single["objects"] = objects
    if title:
        single["vcObjects"] = {"title": [{"properties": {"show": lit(True), "text": lit(title)}}]}
    config = {"name": vid, "layouts": [{"id": 0, "position": {"x": x, "y": y, "z": z, "width": w, "height": h}}],
              "singleVisual": single}
    return {"x": x, "y": y, "z": z, "width": w, "height": h, "config": json.dumps(config), "filters": "[]"}


def textbox(vid: str, pos, text: str, size: int = 12, bold: bool = False) -> dict:
    style = {"fontSize": f"{size}pt"}
    if bold:
        style["fontWeight"] = "bold"
    objects = {"general": [{"properties": {"paragraphs": [{"textRuns": [{"value": text, "textStyle": style}]}]}}]}
    return visual(vid, "textbox", pos, objects=objects)


def card(vid: str, pos, measure: str) -> dict:
    return visual(vid, "card", pos, Q().measure("Values", measure), title=measure)


def pages() -> list[tuple[str, list[dict]]]:
    overview = [
        textbox("ov_title", (24, 12, 900, 44), "Atlanta Bus Reliability (MARTA GTFS-realtime)", 20, True),
        textbox("ov_sub", (24, 52, 1200, 30),
                "On time = −1 to +5 min vs schedule. Gaps are capture outages; from 2026-11-26 capture also pauses "
                "nightly 18:00–01:00 Atlanta time (server offline overnight in Europe).", 10),
        card("ov_c1", (24, 92, 290, 110), "On-time %"),
        card("ov_c2", (330, 92, 290, 110), "GPS-checked on-time %"),
        card("ov_c3", (636, 92, 290, 110), "Arrivals"),
        card("ov_c4", (942, 92, 290, 110), "Median delay (min)"),
        visual("ov_trend", "lineChart", (24, 220, 600, 480),
               Q().col("Category", "dim_date", "date_day").measure("Y", "On-time %"), "On-time % by service day"),
        visual("ov_routes", "clusteredBarChart", (640, 220, 616, 480),
               Q().col("Category", "dim_route", "route_short_name").measure("Y", "On-time % (routes 300+)", sort="asc"),
               "Least reliable routes (300+ arrivals)"),
    ]
    time_of_day = [
        textbox("td_title", (24, 12, 900, 44), "When are buses on time?", 20, True),
        visual("td_slicer", "slicer", (1000, 12, 256, 76), Q().col("Values", "dim_date", "is_weekend"), "Weekend?",
               objects={"data": [{"properties": {"mode": lit("Dropdown")}}],
                        "header": [{"properties": {"show": lit(False)}}]}),
        visual("td_ontime", "clusteredColumnChart", (24, 100, 1232, 300),
               Q().col("Category", "fct_stop_arrival", "scheduled_hour_local", sort="asc").measure("Y", "On-time %"),
               "On-time % by scheduled hour (Atlanta time)"),
        visual("td_mix", "clusteredColumnChart", (24, 410, 1232, 290),
               Q().col("Category", "fct_stop_arrival", "scheduled_hour_local", sort="asc")
               .measure("Y", "Early %").measure("Y", "Late %"),
               "Early vs late by hour"),
    ]
    stops = [
        textbox("st_title", (24, 12, 900, 44), "Stops: where is service reliable?", 20, True),
        visual("st_map", "map", (24, 64, 700, 636),
               Q().col("Category", "dim_stop", "stop_name").col("Y", "dim_stop", "stop_lat", agg=1)
               .col("X", "dim_stop", "stop_lon", agg=1).measure("Size", "Arrivals")
               .measure("Tooltips", "On-time %"),
               "Observed stops (size = arrivals)"),
        visual("st_table", "tableEx", (740, 64, 516, 636),
               Q().col("Values", "dim_stop", "stop_name").measure("Values", "Arrivals")
               .measure("Values", "On-time % (stops 50+)").measure("Values", "Not on time % (stops 50+)", sort="desc"),
               "Least reliable stops (50+ arrivals)"),
    ]
    equity = [
        textbox("eq_title", (24, 12, 1100, 44), "Equity: is service worse where fewer households have a car?", 20,
                True),
        textbox("eq_sub", (24, 52, 1200, 30),
                "One dot per Census tract with 50+ observed arrivals. Car-free share from the ACS 5-year survey.", 10),
        visual("eq_scatter", "scatterChart", (24, 92, 820, 608),
               Q().col("Category", "mart_tract_service_equity", "geoid")
               .col("X", "mart_tract_service_equity", "pct_households_no_vehicle", agg=0)
               .col("Y", "mart_tract_service_equity", "pct_on_time", agg=0),
               "Car-free households % (x) vs on-time % (y)"),
        visual("eq_county", "tableEx", (860, 92, 396, 608),
               Q().col("Values", "mart_tract_service_equity", "county_name")
               .col("Values", "mart_tract_service_equity", "arrivals_observed", agg=0)
               .col("Values", "mart_tract_service_equity", "pct_on_time", agg=1, sort="asc"),
               "By county (average of tracts)"),
    ]
    gps = [
        textbox("gp_title", (24, 12, 1100, 44), "Data quality: does the bus's GPS agree with the feed?", 20, True),
        textbox("gp_sub", (24, 52, 1200, 30),
                "Each feed arrival is matched to the bus's nearest GPS fix (Spark range join). Rows: the feed's "
                "verdict; columns: the GPS verdict; values: % of the row.", 10),
        card("gp_c1", (24, 92, 400, 110), "GPS confirmed %"),
        card("gp_c2", (440, 92, 400, 110), "GPS-checked on-time %"),
        card("gp_c3", (856, 92, 400, 110), "On-time %"),
        visual("gp_matrix", "pivotTable", (24, 220, 1232, 300),
               Q().col("Rows", "mart_gps_validation", "feed_class").col("Columns", "mart_gps_validation", "gps_class")
               .col("Values", "mart_gps_validation", "pct_of_feed_class", agg=0),
               "Feed verdict (rows) vs GPS verdict (columns), % of row",
               objects={"subTotals": [{"properties": {"rowSubtotals": lit(False), "columnSubtotals": lit(False)}}]}),
        visual("gp_counts", "clusteredBarChart", (24, 536, 1232, 164),
               Q().col("Category", "mart_gps_validation", "gps_class").col("Y", "mart_gps_validation", "arrivals",
                                                                          agg=0, sort="desc"),
               "Arrivals by GPS verdict"),
    ]
    return [("Overview", overview), ("Time of day", time_of_day), ("Stops map", stops), ("Equity", equity),
            ("GPS check", gps)]


def report_json() -> dict:
    sections = []
    for i, (name, visuals) in enumerate(pages()):
        for z, v in enumerate(visuals):
            v["z"] = z
        sections.append({"name": f"page{i + 1}", "displayName": name, "displayOption": 1, "width": 1280,
                         "height": 720, "ordinal": i, "config": "{}", "filters": "[]", "visualContainers": visuals})
    config = {"version": "5.59", "themeCollection": {"customTheme": {"name": "atl-transit-theme.json",
                                                                     "version": "5.59", "type": 1}},
              "activeSectionIndex": 0, "linguisticSchemaSyncVersion": 0, "objects": {}}
    return {
        "config": json.dumps(config),
        "layoutOptimization": 0,
        "resourcePackages": [{"resourcePackage": {"name": "RegisteredResources", "type": 1, "disabled": False,
                                                  "items": [{"type": 202, "name": "atl-transit-theme.json",
                                                             "path": "atl-transit-theme.json"}]}}],
        "sections": sections,
    }


# ------------------------------------------------------------------ write the project
def write(out: Path, server: str) -> Path:
    model_dir, report_dir = out / f"{NAME}.SemanticModel", out / f"{NAME}.Report"
    for d in (model_dir, report_dir):
        if d.exists():
            shutil.rmtree(d)
    (report_dir / "StaticResources" / "RegisteredResources").mkdir(parents=True)
    model_dir.mkdir(parents=True)

    measures = parse_measures((HERE / "measures.dax").read_text(encoding="utf-8"))
    dump = lambda p, obj: p.write_text(json.dumps(obj, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")  # noqa: E731
    dump(out / f"{NAME}.pbip", {"version": "1.0", "artifacts": [{"report": {"path": f"{NAME}.Report"}}],
                                "settings": {"enableAutoRecovery": True}})
    dump(model_dir / "definition.pbism", {"version": "1.0", "settings": {}})
    dump(model_dir / "model.bim", semantic_model(server, measures))
    dump(report_dir / "definition.pbir", {"version": "1.0", "datasetReference": {"byPath": {"path": f"../{NAME}.SemanticModel"}}})
    dump(report_dir / "report.json", report_json())
    shutil.copy(HERE / "atl-transit-theme.json", report_dir / "StaticResources" / "RegisteredResources")
    return out / f"{NAME}.pbip"


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--server", default="YOUR-SERVER:5432", help="Postgres host:port as Power BI should reach it")
    ap.add_argument("--out", default=str(HERE / "report"), help="output folder")
    args = ap.parse_args()
    print(write(Path(args.out), args.server))
