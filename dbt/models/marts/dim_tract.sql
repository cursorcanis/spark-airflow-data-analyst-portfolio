{{ config(indexes=[{'columns': ['geoid'], 'unique': True}, {'columns': ['geom'], 'type': 'gist'}]) }}
select
    t.geoid,
    t.county_name,
    t.tract_name,
    round(t.aland_m2 / 1e6, 2) as land_km2,
    a.acs_year,
    a.total_population,
    round(a.total_population / nullif(t.aland_m2 / 1e6, 0)) as population_per_km2,
    a.median_household_income,
    a.households_no_vehicle,
    a.pct_households_no_vehicle,
    a.pct_transit_commute,
    a.poverty_rate,
    t.geom
from {{ ref('stg_census__tracts') }} as t
left join {{ ref('stg_census__acs') }} as a using (geoid)
