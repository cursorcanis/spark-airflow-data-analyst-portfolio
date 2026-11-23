select
    geoid,
    acs_year,
    total_population,
    median_household_income,
    workers_16_plus,
    workers_public_transit,
    occupied_households,
    owner_households_no_vehicle + renter_households_no_vehicle as households_no_vehicle,
    round(100.0 * (owner_households_no_vehicle + renter_households_no_vehicle)
          / nullif(occupied_households, 0), 1) as pct_households_no_vehicle,
    round(100.0 * workers_public_transit / nullif(workers_16_plus, 0), 1) as pct_transit_commute,
    round(100.0 * below_poverty / nullif(poverty_universe, 0), 1) as poverty_rate
from {{ source('raw', 'acs_tract_demographics') }}
