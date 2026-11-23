select geoid, county_fips, county_name, tract_name, aland_m2, awater_m2, geom
from {{ source('raw', 'census_tracts') }}
