{#-
  to_local_time(utc_ts): convert a UTC timestamp WITHOUT time zone into Atlanta wall-clock time
  (also a timestamp without time zone), e.g. 2026-11-26 19:00 (UTC) -> 2026-11-26 14:00 (EST).

  Why two steps? Postgres's AT TIME ZONE does OPPOSITE things depending on the input type:

    timestamptz  AT TIME ZONE 'X'  ->  timestamp   "what does the clock in X show at this instant?"
    timestamp    AT TIME ZONE 'X'  ->  timestamptz "treat this clock reading AS time in X; which instant is it?"

  Our staging columns (scheduled_arrival_at, event_at, ...) are timestamps WITHOUT time zone that hold UTC
  (staging does `raw_ts AT TIME ZONE 'UTC'` on timestamptz raw columns). Writing
  `scheduled_arrival_at AT TIME ZONE 'America/New_York'` therefore used the SECOND meaning: it claimed
  19:00 was already New York time and converted it to the instant 00:00 UTC, so the "local hour" came out
  as 0 instead of 14, off by 10 hours (2 x the 5 h offset, applied in the wrong direction).

  The fix states both facts explicitly:
    1. `utc_ts AT TIME ZONE 'UTC'`               -> timestamptz: "this reading is UTC" (fixes the instant)
    2. `... AT TIME ZONE 'America/New_York'`     -> timestamp:   the Atlanta clock at that instant
  Postgres's tz database applies EST (UTC-5) or EDT (UTC-4) per date, so daylight saving is automatic.
  Pinned by tests/assert_local_time_conversion.sql.
-#}
{% macro to_local_time(utc_ts) -%}
    ((({{ utc_ts }}) at time zone 'UTC') at time zone '{{ var("local_timezone") }}')
{%- endmacro %}
