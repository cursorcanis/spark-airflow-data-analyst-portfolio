{{
  config(
    materialized='incremental',
    unique_key='event_date',
    incremental_strategy='delete+insert',
    indexes=[{'columns': ['event_date']}, {'columns': ['h3_cell']}]
  )
}}
-- Grain: one bus GPS fix. Incremental by whole days: every event_date present in the new batch is
-- deleted and re-inserted (the daily GPS load re-exports the last few days).
select
    event_date,
    event_at,
    vehicle_id,
    trip_id,
    route_id,
    direction_id,
    latitude,
    longitude,
    calc_speed_kmh,
    calc_speed_kmh < {{ var('moving_speed_min_kmh') }} as is_stopped,
    extract(hour from event_at at time zone 'America/New_York')::int as event_hour_local,
    (extract(isodow from event_at at time zone 'America/New_York') < 6
     and extract(hour from event_at at time zone 'America/New_York')::int in ({{ var('peak_hours_local') | join(', ') }}))
        as is_weekday_peak,
    h3_cell::text as h3_cell
from {{ ref('stg_rt__vehicle_pings') }}
{% if is_incremental() %}
where event_date >= (select max(event_date) - {{ var('fct_lookback_days') }} from {{ this }})
{% endif %}
