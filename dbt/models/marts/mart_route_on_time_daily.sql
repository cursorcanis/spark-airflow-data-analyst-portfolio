-- On-time performance per route per service day (the headline transit KPI).
select
    f.service_date,
    f.route_id,
    r.route_short_name,
    r.route_long_name,
    r.route_mode,
    count(*) as arrivals,
    round(100.0 * avg(f.is_on_time::int), 1) as pct_on_time,
    round(100.0 * avg(f.is_late::int), 1) as pct_late,
    round(100.0 * avg(f.is_early::int), 1) as pct_early,
    round((percentile_cont(0.5) within group (order by f.delay_s) / 60.0)::numeric, 1) as median_delay_min,
    round((percentile_cont(0.9) within group (order by f.delay_s) / 60.0)::numeric, 1) as p90_delay_min
from {{ ref('fct_stop_arrival') }} as f
join {{ ref('dim_route') }} as r using (route_id)
group by 1, 2, 3, 4, 5
