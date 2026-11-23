{{ config(indexes=[{'columns': ['geom'], 'type': 'gist'}]) }}
-- Bus speed and reliability per H3 hexagon: where in the region buses crawl, and where they run late.
with speed as (
    select
        h3_cell,
        count(*) as gps_fixes,
        count(distinct route_id) as routes,
        count(calc_speed_kmh) as speed_samples,
        round(100.0 * avg(is_stopped::int), 1) as pct_stopped,
        round((percentile_cont(0.5) within group (order by calc_speed_kmh)
               filter (where not is_stopped))::numeric, 1) as median_moving_speed_kmh,
        round((percentile_cont(0.5) within group (order by calc_speed_kmh)
               filter (where not is_stopped and is_weekday_peak))::numeric, 1) as peak_median_moving_speed_kmh
    from {{ ref('fct_vehicle_ping') }}
    group by 1
),
stop_perf as (
    select
        s.h3_cell,
        count(distinct s.stop_id) as stops_observed,
        count(*) as arrivals_observed,
        round(100.0 * avg(f.is_on_time::int), 1) as pct_on_time
    from {{ ref('fct_stop_arrival') }} as f
    join {{ ref('dim_stop') }} as s using (stop_id)
    group by 1
)
select
    sp.*,
    coalesce(p.stops_observed, 0) as stops_observed,
    coalesce(p.arrivals_observed, 0) as arrivals_observed,
    p.pct_on_time,
    h3_cell_to_boundary_geometry(sp.h3_cell::h3index) as geom
from speed as sp
left join stop_perf as p using (h3_cell)
