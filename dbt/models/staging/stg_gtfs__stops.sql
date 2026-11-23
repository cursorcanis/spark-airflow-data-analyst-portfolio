select
    stop_id,
    stop_code,
    stop_name,
    stop_lat,
    stop_lon,
    location_type,
    parent_station,
    wheelchair_boarding,
    -- longitude first! (x, y)
    st_setsrid(st_makepoint(stop_lon, stop_lat), 4326)::geometry(Point, 4326) as geom
from {{ source('raw', 'gtfs_stops') }}
where stop_lat is not null and stop_lon is not null
