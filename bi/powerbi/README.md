# Power BI report (Power BI Desktop, free)

The report is built by hand in Power BI Desktop on a Windows PC, against the **marts** schema, using a
**read-only** login. Everything you need is here: connection steps, the model, the measures
([measures.dax](measures.dax)) and a color theme ([atl-transit-theme.json](atl-transit-theme.json)).

## Fast path: generate the whole report (report as code)

`build_pbip.py` writes the complete Power BI Project from code: the connection (a `PgServer` parameter), the 10 tables
in Import mode with geometry columns left out, the 4 relationships, `dim_date` marked as the date table, every
measure in [measures.dax](measures.dax) and 5 finished pages (Overview, Time of day, Stops map, Equity, GPS check).

```bash
python bi/powerbi/build_pbip.py --server <server-tailscale-ip>:5432 --out bi/powerbi/local   # gitignored copy
```
1. Open `bi\powerbi\local\ATL Transit.pbip` in Power BI Desktop (Windows Tailscale must be connected).
2. **Refresh**, then enter credentials once: **Database** tab, user `reporting` (password: step 2 below).
3. Stops map: map visuals are off by default. Turn them on in **File > Options and settings > Options >
   Global > Security > Map and filled map visuals**.
4. **Ctrl+S** keeps the loaded data in the local copy.

The committed `report/` folder is the same project with a placeholder server address. Change it in
**Transform data > Edit parameters > PgServer**. The manual steps below explain what the generator builds.

## 1. Connect

1. Install **Power BI Desktop** (Microsoft Store, free).
2. Get the password of the read-only `reporting` login from the server's `.env` (it never leaves it otherwise):
   `ssh <user>@<server> grep REPORTING_DB_PASSWORD <repo-dir>/.env`
3. **Get data > PostgreSQL database**
   - Server: `<server Tailscale IP>:5432`  (Postgres listens only on the Tailscale interface)
   - Database: `warehouse`
   - Data connectivity mode: **Import** (fast visuals; refresh pulls a new copy)
   - Credentials: **Database** tab, user `reporting`
4. If it complains about encryption: **File > Options and settings > Data source settings > Edit permissions >**
   uncheck **Encrypt connections**. The traffic already travels inside Tailscale's WireGuard tunnel.

`reporting` can only `SELECT` from `marts` (dbt `grants` config), its sessions are read-only
(`default_transaction_read_only`) and queries stop after 120 s (`statement_timeout`).

## 2. Pick tables (Navigator)

`marts.dim_route`, `dim_stop`, `dim_date`, `dim_tract`, `fct_stop_arrival`, `mart_route_on_time_daily`,
`mart_on_time_by_hour`, `mart_tract_service_equity`, `mart_gps_validation`, `mart_h3_bus_speed`.

Click **Transform data** and, in Power Query, **remove the `geom` columns** (PostGIS geometries arrive as long
text; the web map handles geometry). Skip `fct_vehicle_ping` (millions of GPS rows; not needed for this report).

## 3. Model (Model view)

| From (many) | To (one) |
|---|---|
| `fct_stop_arrival[route_id]` | `dim_route[route_id]` |
| `fct_stop_arrival[stop_id]` | `dim_stop[stop_id]` |
| `fct_stop_arrival[service_date]` | `dim_date[date_day]` |
| `dim_stop[tract_geoid]` | `dim_tract[geoid]` |

- Single-direction filters (dimension → fact): a star schema.
- `dim_date`: **Table tools > Mark as date table** (`date_day`), so time-intelligence DAX works.
- Hide the key columns on the fact table so report authors use the dimensions.

## 4. Measures

Create a `_Measures` table and add every measure from [measures.dax](measures.dax).
**View > Themes > Browse for themes** → `atl-transit-theme.json`.

## 5. Pages

1. **Overview**: cards (`On-time %`, `GPS-checked on-time %`, `Arrivals`, `Median delay (min)`); line chart
   `On-time %` by `dim_date[date_day]`; bar chart of the 15 routes with the lowest `On-time %`
   (Top N filter, at least 300 `Arrivals`).
2. **Time of day**: column chart `On-time %` by `fct_stop_arrival[scheduled_hour_local]`; slicer
   `dim_date[is_weekend]`.
3. **Stops map**: Map / Azure Maps visual with `dim_stop[stop_lat]`, `stop_lon`, size = `Arrivals`,
   color = `On-time %` (diverging, center 70 %).
4. **Equity**: scatter of `mart_tract_service_equity`: X = `pct_households_no_vehicle`, Y = `pct_on_time`,
   size = `total_population`; filter `arrivals_observed >= 50`.
5. **Data quality**: matrix from `mart_gps_validation`: rows `feed_class`, columns `gps_class`, values
   `arrivals` (show as % of row total). This shows how many of the feed's "early" arrivals the GPS track confirms.

## 6. Save in a git-friendly format

**File > Save as > Power BI project (.pbip)** into `bi/powerbi/report/`. A .pbip saves the model as text (TMDL)
and the report as JSON, so changes show up in `git diff`. Local cache files are gitignored.

## Refreshing

**Home > Refresh** re-imports the marts. Scheduled refresh in the Power BI *Service* needs an
on-premises data gateway on a machine that can reach the server (optional; not needed for the portfolio).
