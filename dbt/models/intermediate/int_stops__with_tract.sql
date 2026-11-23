{{ config(indexes=[{'columns': ['stop_id'], 'unique': True}]) }}
-- Point-in-polygon: which Census tract each stop is in (uses the GiST index on census_tracts.geom).
-- ST_Intersects (not ST_Contains) so a stop exactly on a boundary still matches; DISTINCT ON keeps one tract.
select distinct on (s.stop_id)
    s.stop_id,
    t.geoid as tract_geoid,
    t.county_name
from {{ ref('stg_gtfs__stops') }} as s
left join {{ ref('stg_census__tracts') }} as t
    on st_intersects(t.geom, s.geom)
order by s.stop_id, t.geoid
