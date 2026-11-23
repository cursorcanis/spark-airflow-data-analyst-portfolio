select
    route_id,
    route_short_name,
    route_long_name,
    route_type,
    case route_type
        when 0 then 'tram' when 1 then 'subway' when 2 then 'rail' when 3 then 'bus' else 'other'
    end as route_mode,
    route_color,
    feed_ingest_date
from {{ source('raw', 'gtfs_routes') }}
