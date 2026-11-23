-- When in the day does service slip? (scheduled hour in Atlanta local time, weekday vs weekend)
select
    f.scheduled_hour_local,
    d.is_weekend,
    count(*) as arrivals,
    round(100.0 * avg(f.is_on_time::int), 1) as pct_on_time,
    round((percentile_cont(0.5) within group (order by f.delay_s) / 60.0)::numeric, 1) as median_delay_min
from {{ ref('fct_stop_arrival') }} as f
join {{ ref('dim_date') }} as d on d.date_day = f.service_date
group by 1, 2
