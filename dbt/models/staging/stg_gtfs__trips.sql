select trip_id, route_id, service_id, direction_id, trip_headsign, shape_id, block_id
from {{ source('raw', 'gtfs_trips') }}
