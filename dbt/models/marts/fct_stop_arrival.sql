{{
  config(
    materialized='incremental',
    unique_key=['service_date', 'trip_id', 'stop_sequence'],
    incremental_strategy='delete+insert',
    indexes=[{'columns': ['service_date']}, {'columns': ['route_id']}, {'columns': ['stop_id']}]
  )
}}
-- Grain: one row per (service_date, trip, stop_sequence) = one observed arrival at a scheduled stop.
-- Incremental: only the last N service dates are rebuilt (delete+insert), matching the load lookback window.
with arrivals as (
    select *
    from {{ ref('stg_rt__stop_arrivals') }}
    where has_schedule
      and abs(delay_s) <= {{ var('max_abs_delay_s') }}
    {% if is_incremental() %}
      and service_date >= (select max(service_date) - {{ var('fct_lookback_days') }} from {{ this }})
    {% endif %}
)
select
    a.service_date,
    a.trip_id,
    a.stop_sequence,
    a.route_id,
    a.stop_id,
    t.direction_id,
    a.vehicle_id,
    a.scheduled_arrival_at,
    a.observed_arrival_at,
    a.delay_s,
    round(a.delay_s / 60.0, 2) as delay_min,
    -- scheduled_arrival_at is UTC without a time zone: convert via the macro (see macros/to_local_time.sql)
    extract(hour from {{ to_local_time('a.scheduled_arrival_at') }})::int as scheduled_hour_local,
    a.delay_s between {{ var('on_time_early_s') }} and {{ var('on_time_late_s') }} as is_on_time,
    a.delay_s > {{ var('on_time_late_s') }} as is_late,
    a.delay_s < {{ var('on_time_early_s') }} as is_early
from arrivals as a
left join {{ ref('stg_gtfs__trips') }} as t using (trip_id)
