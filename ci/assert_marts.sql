-- Checks on the fixture's known answers (psql exits non-zero on the first failed check via ON_ERROR_STOP + division by zero).
\set ON_ERROR_STOP on
-- 3 scheduled arrivals kept, unscheduled one dropped
SELECT 1 / (CASE WHEN (SELECT count(*) FROM marts.fct_stop_arrival) = 3 THEN 1 ELSE 0 END) AS fct_rows_ok;
-- classification: 1 on time, 1 late, 1 early
SELECT 1 / (CASE WHEN (SELECT count(*) FILTER (WHERE is_on_time) || '-' || count(*) FILTER (WHERE is_late)
                       || '-' || count(*) FILTER (WHERE is_early) FROM marts.fct_stop_arrival) = '1-1-1'
            THEN 1 ELSE 0 END) AS classification_ok;
-- spatial join: Five Points stop inside the tract, the far stop outside
SELECT 1 / (CASE WHEN (SELECT tract_geoid FROM marts.dim_stop WHERE stop_id = 'S1') = '13121011901'
                  AND (SELECT tract_geoid FROM marts.dim_stop WHERE stop_id = 'S2') IS NULL THEN 1 ELSE 0 END) AS spatial_ok;
-- ACS derived metric: (20 + 394) / 1000 = 41.4 %
SELECT 1 / (CASE WHEN (SELECT pct_households_no_vehicle FROM marts.dim_tract) = 41.4 THEN 1 ELSE 0 END) AS acs_ok;
-- scheduled hour in Atlanta local time: 13:00 UTC = 08 EST
SELECT 1 / (CASE WHEN (SELECT scheduled_hour_local FROM marts.fct_stop_arrival WHERE trip_id='T1' AND stop_sequence=1) = 8
            THEN 1 ELSE 0 END) AS local_hour_ok;
-- same for GPS fixes: 13:01 UTC on Monday 2026-11-23 = 08:01 EST -> hour 8, a weekday peak hour.
-- (The fixture's raw timestamps are timestamptz, exactly like production; the 2026-11-26 local-hour bug
--  passed CI because the fixture used plain timestamp and staging then produced a different column type.)
SELECT 1 / (CASE WHEN (SELECT bool_and(event_hour_local = 8 AND is_weekday_peak) FROM marts.fct_vehicle_ping)
            THEN 1 ELSE 0 END) AS ping_local_hour_ok;
-- GPS validation: the feed's "early" arrival is on time by GPS; the late one has no GPS confirmation
SELECT 1 / (CASE WHEN (SELECT arrivals FROM marts.mart_gps_validation WHERE feed_class = 'early' AND gps_class = 'on_time') = 1
                  AND (SELECT arrivals FROM marts.mart_gps_validation WHERE feed_class = 'late' AND gps_class = 'not_confirmed') = 1
            THEN 1 ELSE 0 END) AS gps_validation_ok;
-- segment delay: T1 is 120 s late at S1 and 600 s late at S2 -> 480 s added on S1 -> S2
SELECT 1 / (CASE WHEN (SELECT avg_delay_added_s FROM marts.mart_segment_delay WHERE from_stop_id = 'S1' AND to_stop_id = 'S2') = 480
            THEN 1 ELSE 0 END) AS segment_delay_ok;
-- H3: Five Points' hexagon has the 3 GPS fixes (1 of 2 speed samples stopped) and both S1 arrivals
SELECT 1 / (CASE WHEN (SELECT gps_fixes || '-' || pct_stopped || '-' || median_moving_speed_kmh || '-' || arrivals_observed
                       FROM marts.mart_h3_bus_speed
                       WHERE h3_cell = (SELECT h3_cell FROM marts.dim_stop WHERE stop_id = 'S1')) = '3-50.0-12.0-2'
            THEN 1 ELSE 0 END) AS h3_ok;
-- grants: the read-only BI role can SELECT marts
SELECT 1 / (CASE WHEN has_table_privilege('reporting', 'marts.fct_stop_arrival', 'SELECT')
                  AND has_table_privilege('reporting', 'marts.mart_h3_bus_speed', 'SELECT') THEN 1 ELSE 0 END) AS grants_ok;
