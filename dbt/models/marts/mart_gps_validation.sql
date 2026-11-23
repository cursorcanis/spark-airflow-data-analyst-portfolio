-- Is the feed's "arrival" real? Compare each arrival's on-time class from the feed with the class you get
-- when the bus's own GPS pass time is used instead (gps_delay_s = delay_s - gps_offset_s).
-- A confusion matrix: feed_class x gps_class.
with joined as (
    select
        case when f.is_early then 'early' when f.is_late then 'late' else 'on_time' end as feed_class,
        g.gps_confirmed,
        g.gps_offset_s,
        g.gps_min_dist_m,
        case when g.gps_confirmed then f.delay_s - g.gps_offset_s end as gps_delay_s
    from {{ ref('fct_stop_arrival') }} as f
    join {{ ref('stg_rt__arrival_gps_check') }} as g using (service_date, trip_id, stop_sequence)
),
classified as (
    select
        *,
        case
            when not gps_confirmed then 'not_confirmed'
            when gps_delay_s < {{ var('on_time_early_s') }} then 'early'
            when gps_delay_s > {{ var('on_time_late_s') }} then 'late'
            else 'on_time'
        end as gps_class
    from joined
)
select
    feed_class,
    gps_class,
    count(*) as arrivals,
    round(100.0 * count(*) / sum(count(*)) over (partition by feed_class), 1) as pct_of_feed_class,
    round((percentile_cont(0.5) within group (order by gps_offset_s))::numeric) as median_gps_offset_s,
    round((percentile_cont(0.5) within group (order by gps_min_dist_m))::numeric) as median_gps_dist_m
from classified
group by 1, 2
