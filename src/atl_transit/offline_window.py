"""A daily window when the server is known to have no internet (e.g. the home router is off at night).

During the window the poller doesn't call MARTA (every request would fail) and the heartbeat stays quiet
(stale data is expected, not an incident). Configured with two environment variables:

    OFFLINE_WINDOW=00:00-07:00        local start-end, may wrap past midnight (e.g. 23:30-06:00); empty = none
    OFFLINE_WINDOW_TZ=Europe/Rome     the time zone the window is written in (the server's local time)
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo


@dataclass(frozen=True)
class OfflineWindow:
    start: time
    end: time
    tz: ZoneInfo

    @classmethod
    def parse(cls, spec: str, tz: str) -> OfflineWindow | None:
        spec = spec.strip()
        if not spec:
            return None
        try:
            start_s, end_s = spec.split("-")
            start, end = time.fromisoformat(start_s.strip()), time.fromisoformat(end_s.strip())
        except ValueError as exc:
            raise ValueError(f"OFFLINE_WINDOW must look like 00:00-07:00, got {spec!r}") from exc
        if start == end:
            raise ValueError("OFFLINE_WINDOW start and end must differ")
        return cls(start, end, ZoneInfo(tz))

    @classmethod
    def from_env(cls) -> OfflineWindow | None:
        return cls.parse(os.environ.get("OFFLINE_WINDOW", ""), os.environ.get("OFFLINE_WINDOW_TZ", "UTC"))

    def contains(self, moment: datetime, grace_after: timedelta = timedelta(0)) -> bool:
        """True if `moment` (timezone-aware) is inside the window, or within `grace_after` of its end."""
        local = moment.astimezone(self.tz)
        if self._inside(local.time()):
            return True
        return grace_after > timedelta(0) and self._inside((local - grace_after).time())

    def fraction_of(self, start: datetime, end: datetime) -> float:
        """Share of [start, end) inside the window, sampled per minute (exact for windows on minute boundaries)."""
        minutes = int((end - start) / timedelta(minutes=1))
        if minutes <= 0:
            return 0.0
        inside = sum(self.contains(start + timedelta(minutes=m)) for m in range(minutes))
        return inside / minutes

    def _inside(self, t: time) -> bool:
        if self.start < self.end:
            return self.start <= t < self.end
        return t >= self.start or t < self.end  # wraps past midnight

    def __str__(self) -> str:
        return f"{self.start:%H:%M}-{self.end:%H:%M} {self.tz.key}"
