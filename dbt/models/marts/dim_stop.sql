{{ config(indexes=[{'columns': ['stop_id'], 'unique': True}, {'columns': ['geom'], 'type': 'gist'}]) }}
select
    s.stop_id,
    s.stop_code,
    s.stop_name,
    s.stop_lat,
    s.stop_lon,
    (s.location_type = 1 or s.parent_station is not null or s.stop_name ilike '%station%') as is_rail_station_area,
    s.wheelchair_boarding = 1 as is_wheelchair_accessible,
    st.tract_geoid,
    st.county_name,
    h3_lat_lng_to_cell(s.geom, {{ var('h3_resolution') }})::text as h3_cell,
    s.geom
from {{ ref('stg_gtfs__stops') }} as s
left join {{ ref('int_stops__with_tract') }} as st using (stop_id)
