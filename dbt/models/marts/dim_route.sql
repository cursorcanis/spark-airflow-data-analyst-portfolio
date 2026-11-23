select
    r.route_id,
    r.route_short_name,
    r.route_long_name,
    r.route_mode,
    r.route_color,
    count(t.trip_id) as scheduled_trips_in_feed
from {{ ref('stg_gtfs__routes') }} as r
left join {{ ref('stg_gtfs__trips') }} as t using (route_id)
group by 1, 2, 3, 4, 5
