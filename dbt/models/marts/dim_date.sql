-- Calendar covering every observed service date, plus padding.
with bounds as (
    select min(service_date) as first_day, max(service_date) as last_day
    from {{ ref('stg_rt__stop_arrivals') }}
)
select
    d::date as date_day,
    extract(isodow from d)::int as iso_day_of_week,
    to_char(d, 'Dy') as day_name,
    extract(isodow from d) in (6, 7) as is_weekend,
    extract(month from d)::int as month,
    extract(year from d)::int as year
from bounds, generate_series(bounds.first_day - 7, bounds.last_day + 30, interval '1 day') as d
