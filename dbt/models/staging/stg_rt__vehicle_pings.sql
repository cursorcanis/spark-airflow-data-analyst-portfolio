-- Bus GPS fixes. Each one gets its H3 cell: the hexagon (at var h3_resolution) that contains it.
select
    event_date,
    event_ts at time zone 'UTC' as event_at,
    vehicle_id,
    trip_id,
    route_id,
    direction_id,
    latitude,
    longitude,
    round((calc_speed_mps * 3.6)::numeric, 1) as calc_speed_kmh,
    secs_since_prev,
    h3_lat_lng_to_cell(
        st_setsrid(st_makepoint(longitude, latitude), 4326), {{ var('h3_resolution') }}
    ) as h3_cell
from {{ source('raw', 'rt_vehicle_pings') }}
