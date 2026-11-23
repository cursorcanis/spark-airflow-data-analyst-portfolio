select
    service_date,
    trip_id,
    stop_sequence,
    stop_id,
    gps_pings_in_window,
    gps_min_dist_m,
    gps_closest_ts at time zone 'UTC' as gps_closest_at,
    gps_offset_s,          -- feed-reported arrival minus GPS pass time (negative = feed says earlier)
    gps_confirmed          -- a GPS fix within 150 m of the stop exists
from {{ source('raw', 'rt_arrival_gps_check') }}
