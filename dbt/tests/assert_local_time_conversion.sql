-- Pins the UTC -> Atlanta conversion (macro to_local_time) to known answers. Returns rows = failures.
-- Guards against the 2026-11-26 bug, where a UTC timestamp without time zone was converted in the wrong
-- direction and every "local hour" was 10 hours off.
with cases (utc_naive, expected_local, why) as (
    values
        (timestamp '2026-11-26 19:00', timestamp '2026-11-26 14:00', 'winter: EST = UTC-5'),
        (timestamp '2026-07-01 12:00', timestamp '2026-07-01 08:00', 'summer: EDT = UTC-4'),
        (timestamp '2026-11-27 03:30', timestamp '2026-11-26 22:30', 'crosses midnight: previous local day')
)
select
    why,
    utc_naive,
    expected_local,
    {{ to_local_time('utc_naive') }} as actual_local
from cases
where {{ to_local_time('utc_naive') }} <> expected_local
