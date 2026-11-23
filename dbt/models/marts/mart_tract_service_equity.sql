{{ config(indexes=[{'columns': ['geom'], 'type': 'gist'}]) }}
-- Service reliability by neighborhood vs. who lives there (equity analysis, ready to map).
with stop_perf as (
    select
        s.tract_geoid,
        count(distinct s.stop_id) as stops_observed,
        count(*) as arrivals_observed,
        round(100.0 * avg(f.is_on_time::int), 1) as pct_on_time,
        round((percentile_cont(0.5) within group (order by f.delay_s) / 60.0)::numeric, 1) as median_delay_min
    from {{ ref('fct_stop_arrival') }} as f
    join {{ ref('dim_stop') }} as s using (stop_id)
    where s.tract_geoid is not null
    group by 1
)
select
    t.geoid,
    t.county_name,
    t.total_population,
    t.median_household_income,
    t.pct_households_no_vehicle,
    t.pct_transit_commute,
    t.poverty_rate,
    coalesce(p.stops_observed, 0) as stops_observed,
    coalesce(p.arrivals_observed, 0) as arrivals_observed,
    p.pct_on_time,
    p.median_delay_min,
    t.geom
from {{ ref('dim_tract') }} as t
left join stop_perf as p on p.tract_geoid = t.geoid
