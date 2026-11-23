-- Singular test: stops with observed arrivals should almost all fall inside the 11-county tract layer.
-- Returns a row (= failure) if more than 5% are unmapped. Warn only: a few MARTA stops sit outside it.
{{ config(severity='warn') }}
with observed as (
    select distinct s.stop_id, s.tract_geoid
    from {{ ref('fct_stop_arrival') }} as f
    join {{ ref('dim_stop') }} as s using (stop_id)
)
select count(*) filter (where tract_geoid is null) as unmapped, count(*) as total
from observed
having count(*) filter (where tract_geoid is null) > 0.05 * count(*)
