select trip_id, stop_sequence, stop_id, arrival_secs, departure_secs, timepoint
from {{ source('raw', 'gtfs_stop_times') }}
