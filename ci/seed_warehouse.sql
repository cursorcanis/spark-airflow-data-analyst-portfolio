-- Minimal raw-schema fixture for running `dbt build` in CI (same column names/types the Spark JDBC
-- loads and the Census DAG create in production). Tiny but covers every join and test path.
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS h3;
CREATE EXTENSION IF NOT EXISTS h3_postgis CASCADE;
CREATE ROLE reporting LOGIN;            -- dbt `grants` on marts need the role to exist
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

CREATE TABLE raw.gtfs_routes (route_id text, agency_id text, route_short_name text, route_long_name text,
    route_type int, route_color text, route_text_color text, feed_ingest_date date);
INSERT INTO raw.gtfs_routes VALUES
    ('R1', 'MARTA', '1', 'Marietta Blvd/Joseph E Lowery', 3, 'FF0000', 'FFFFFF', '2026-11-22'),
    ('R2', 'MARTA', 'RED', 'Red Line', 1, 'CE242B', 'FFFFFF', '2026-11-22');

CREATE TABLE raw.gtfs_stops (stop_id text, stop_code text, stop_name text, stop_desc text, stop_lat double precision,
    stop_lon double precision, location_type int, parent_station text, wheelchair_boarding int, feed_ingest_date date);
INSERT INTO raw.gtfs_stops VALUES
    ('S1', 'S1', 'FIVE POINTS STATION', NULL, 33.7539, -84.3916, NULL, '900', 1, '2026-11-22'),
    ('S2', 'S2', 'FAR AWAY STOP', NULL, 34.6000, -83.6000, NULL, NULL, 0, '2026-11-22');

CREATE TABLE raw.gtfs_trips (route_id text, service_id text, trip_id text, trip_headsign text, direction_id int,
    block_id text, shape_id text, wheelchair_accessible int, feed_ingest_date date);
INSERT INTO raw.gtfs_trips VALUES
    ('R1', 'WK', 'T1', 'Downtown', 0, 'B1', 'SH1', 1, '2026-11-22'),
    ('R2', 'WK', 'T2', 'Airport', 1, 'B2', 'SH2', 1, '2026-11-22');

CREATE TABLE raw.gtfs_stop_times (trip_id text, arrival_time text, departure_time text, stop_id text,
    stop_sequence int, pickup_type int, drop_off_type int, shape_dist_traveled double precision, timepoint int,
    arrival_secs int, departure_secs int, feed_ingest_date date);
INSERT INTO raw.gtfs_stop_times VALUES
    ('T1', '08:00:00', '08:00:00', 'S1', 1, 0, 0, 0, 1, 28800, 28800, '2026-11-22'),
    ('T1', '08:10:00', '08:10:00', 'S2', 2, 0, 0, 1, 1, 29400, 29400, '2026-11-22'),
    ('T2', '09:00:00', '09:00:00', 'S1', 1, 0, 0, 0, 1, 32400, 32400, '2026-11-22');

CREATE TABLE raw.gtfs_calendar (service_id text, monday int, tuesday int, wednesday int, thursday int, friday int,
    saturday int, sunday int, start_date date, end_date date, feed_ingest_date date);
CREATE TABLE raw.gtfs_calendar_dates (service_id text, date date, exception_type int, feed_ingest_date date);
CREATE TABLE raw.gtfs_shapes (shape_id text, shape_pt_lat double precision, shape_pt_lon double precision,
    shape_pt_sequence int, shape_dist_traveled double precision, feed_ingest_date date);

-- Timestamps are UTC without zone, exactly as Spark's JDBC writer creates them.
CREATE TABLE raw.rt_stop_arrivals (service_date date, trip_id text, stop_sequence int, route_id text, stop_id text,
    rt_stop_id text, vehicle_id text, observed_arrival_ts timestamp, scheduled_arrival_ts timestamp, delay_s int,
    has_schedule boolean, feed_ts bigint);
INSERT INTO raw.rt_stop_arrivals VALUES
    ('2026-11-23', 'T1', 1, 'R1', 'S1', '9997', 'V1', '2026-11-23 13:02:00', '2026-11-23 13:00:00', 120, true, 1795438000),   -- on time
    ('2026-11-23', 'T1', 2, 'R1', 'S2', '9998', 'V1', '2026-11-23 13:20:00', '2026-11-23 13:10:00', 600, true, 1795438900),   -- late
    ('2026-11-23', 'T2', 1, 'R2', 'S1', '9997', 'V2', '2026-11-23 13:57:00', '2026-11-23 14:00:00', -180, true, 1795442000),  -- early
    ('2026-11-23', 'TX', 1, 'R9', 'S9', 'S9', 'V9', '2026-11-23 15:00:00', NULL, NULL, false, 1795446000);                   -- unscheduled

CREATE TABLE raw.census_tracts (geoid text PRIMARY KEY, state_fips text, county_fips text, county_name text,
    tract_code text, tract_name text, aland_m2 bigint, awater_m2 bigint, intpt_lat double precision,
    intpt_lon double precision, geom geometry(MultiPolygon, 4326), loaded_at timestamptz DEFAULT now());
INSERT INTO raw.census_tracts (geoid, state_fips, county_fips, county_name, tract_code, tract_name, aland_m2, awater_m2,
    intpt_lat, intpt_lon, geom) VALUES
    ('13121011901', '13', '121', 'Fulton', '011901', 'Census Tract 119.01', 1500000, 0, 33.754, -84.391,
     ST_Multi(ST_GeomFromText('POLYGON((-84.40 33.74, -84.38 33.74, -84.38 33.76, -84.40 33.76, -84.40 33.74))', 4326)));

CREATE TABLE raw.acs_tract_demographics (geoid text PRIMARY KEY, acs_year int, total_population int,
    median_household_income int, workers_16_plus int, workers_public_transit int, occupied_households int,
    owner_households_no_vehicle int, renter_households_no_vehicle int, poverty_universe int, below_poverty int,
    loaded_at timestamptz DEFAULT now());
INSERT INTO raw.acs_tract_demographics (geoid, acs_year, total_population, median_household_income, workers_16_plus,
    workers_public_transit, occupied_households, owner_households_no_vehicle, renter_households_no_vehicle,
    poverty_universe, below_poverty) VALUES
    ('13121011901', 2024, 2184, 77838, 1200, 300, 1000, 20, 394, 2100, 400);

-- GPS fixes for trip T1 around stop S1 (Five Points). Speeds are what Spark computes; 3.33 m/s = 12 km/h.
CREATE TABLE raw.rt_vehicle_pings (event_date date, event_ts timestamp, vehicle_ts bigint, vehicle_id text,
    trip_id text, route_id text, direction_id int, latitude double precision, longitude double precision,
    reported_speed_mps double precision, calc_speed_mps double precision, secs_since_prev int);
INSERT INTO raw.rt_vehicle_pings VALUES
    ('2026-11-23', '2026-11-23 13:01:30', 1795438890, 'V1', 'T1', 'R1', 0, 33.7539, -84.3916, NULL, NULL, NULL),
    ('2026-11-23', '2026-11-23 13:02:00', 1795438920, 'V1', 'T1', 'R1', 0, 33.7541, -84.3916, NULL, 3.33, 30),
    ('2026-11-23', '2026-11-23 13:02:30', 1795438950, 'V1', 'T1', 'R1', 0, 33.7541, -84.3916, NULL, 0.0, 30);

-- GPS check per arrival. gps_offset_s = feed arrival - GPS pass. T2's "early" (-180 s) is really -30 s by GPS.
CREATE TABLE raw.rt_arrival_gps_check (service_date date, trip_id text, stop_sequence int, stop_id text,
    observed_arrival_ts timestamp, gps_pings_in_window bigint, gps_min_dist_m double precision,
    gps_closest_ts timestamp, gps_offset_s int, gps_confirmed boolean);
INSERT INTO raw.rt_arrival_gps_check VALUES
    ('2026-11-23', 'T1', 1, 'S1', '2026-11-23 13:02:00', 3, 5.0, '2026-11-23 13:01:30', 30, true),     -- on time either way
    ('2026-11-23', 'T1', 2, 'S2', '2026-11-23 13:20:00', 0, NULL, NULL, NULL, false),                  -- no GPS
    ('2026-11-23', 'T2', 1, 'S1', '2026-11-23 13:57:00', 40, 20.0, '2026-11-23 13:59:30', -150, true); -- early -> on time
