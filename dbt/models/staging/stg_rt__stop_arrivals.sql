-- Spark writes UTC timestamps without a zone; make them explicit timestamptz here.
select
    service_date,
    trip_id,
    stop_sequence,
    route_id,
    stop_id,
    rt_stop_id,
    vehicle_id,
    observed_arrival_ts at time zone 'UTC' as observed_arrival_at,
    scheduled_arrival_ts at time zone 'UTC' as scheduled_arrival_at,
    delay_s,
    has_schedule,
    feed_ts
from {{ source('raw', 'rt_stop_arrivals') }}
