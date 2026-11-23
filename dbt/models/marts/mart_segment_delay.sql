{{ config(indexes=[{'columns': ['geom'], 'type': 'gist'}]) }}
-- Where do buses LOSE time? For consecutive observed stops of the same trip, delay_added = how much later
-- (vs schedule) the bus is at the next stop than at this one. Grain: one directed stop-to-stop segment.
with ordered as (
    select
        f.service_date,
        f.trip_id,
        f.route_id,
        f.stop_id as to_stop_id,
        f.stop_sequence,
        f.delay_s,
        lag(f.stop_id) over w as from_stop_id,
        lag(f.stop_sequence) over w as prev_sequence,
        lag(f.delay_s) over w as prev_delay_s
    from {{ ref('fct_stop_arrival') }} as f
    window w as (partition by f.service_date, f.trip_id order by f.stop_sequence)
),
traversals as (
    select *, delay_s - prev_delay_s as delay_added_s
    from ordered
    where prev_sequence = stop_sequence - 1     -- only truly consecutive stops (no gaps in the feed)
)
select
    t.from_stop_id,
    t.to_stop_id,
    a.stop_name as from_stop_name,
    b.stop_name as to_stop_name,
    string_agg(distinct r.route_short_name, ', ' order by r.route_short_name) as routes,
    count(*) as traversals,
    round(avg(t.delay_added_s)) as avg_delay_added_s,
    round((percentile_cont(0.5) within group (order by t.delay_added_s))::numeric) as median_delay_added_s,
    round(st_length(st_makeline(a.geom, b.geom)::geography)) as length_m,
    st_makeline(a.geom, b.geom) as geom
from traversals as t
join {{ ref('dim_stop') }} as a on a.stop_id = t.from_stop_id
join {{ ref('dim_stop') }} as b on b.stop_id = t.to_stop_id
join {{ ref('dim_route') }} as r on r.route_id = t.route_id
group by 1, 2, 3, 4, a.geom, b.geom
