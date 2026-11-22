"""Helpers for downloading and validating GTFS static feeds.

Pure Python (no Airflow imports) so the logic is unit-testable on its own.
"""

from __future__ import annotations

import csv
import hashlib
import io
import zipfile
from dataclasses import asdict, dataclass, field
from pathlib import Path

# Files required by the GTFS reference (calendar.txt OR calendar_dates.txt must be present).
REQUIRED_FILES = frozenset({"agency.txt", "stops.txt", "routes.txt", "trips.txt", "stop_times.txt"})
CALENDAR_FILES = frozenset({"calendar.txt", "calendar_dates.txt"})


class GtfsValidationError(ValueError):
    """Raised when a GTFS archive is unusable."""


@dataclass
class GtfsSummary:
    sha256: str
    size_bytes: int
    row_counts: dict[str, int] = field(default_factory=dict)
    feed_start_date: str | None = None
    feed_end_date: str | None = None
    feed_version: str | None = None

    def to_dict(self) -> dict:
        return asdict(self)


def sha256_file(path: str | Path, chunk_size: int = 1 << 20) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        while chunk := fh.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def _count_rows(zf: zipfile.ZipFile, name: str) -> int:
    with zf.open(name) as raw:
        text = io.TextIOWrapper(raw, encoding="utf-8-sig", newline="")
        reader = csv.reader(text)
        next(reader, None)  # header
        return sum(1 for _ in reader)


def _read_feed_info(zf: zipfile.ZipFile) -> dict[str, str]:
    if "feed_info.txt" not in zf.namelist():
        return {}
    with zf.open("feed_info.txt") as raw:
        rows = list(csv.DictReader(io.TextIOWrapper(raw, encoding="utf-8-sig", newline="")))
    return {k.strip(): (v or "").strip() for k, v in rows[0].items()} if rows else {}


def validate_gtfs_zip(path: str | Path) -> GtfsSummary:
    """Check a GTFS zip for required files and non-empty core tables; return a summary."""
    path = Path(path)
    if not zipfile.is_zipfile(path):
        raise GtfsValidationError(f"{path.name} is not a zip archive")

    with zipfile.ZipFile(path) as zf:
        # Some feeds nest files in a folder; GTFS requires them at the root.
        names = {n for n in zf.namelist() if not n.endswith("/") and "/" not in n}

        missing = sorted(REQUIRED_FILES - names)
        if missing:
            raise GtfsValidationError(f"missing required files: {', '.join(missing)}")
        if not names & CALENDAR_FILES:
            raise GtfsValidationError("needs calendar.txt or calendar_dates.txt")

        row_counts = {name: _count_rows(zf, name) for name in sorted(n for n in names if n.endswith(".txt"))}
        empty = sorted(n for n in REQUIRED_FILES if row_counts.get(n, 0) == 0)
        if empty:
            raise GtfsValidationError(f"required files have no rows: {', '.join(empty)}")

        info = _read_feed_info(zf)

    return GtfsSummary(
        sha256=sha256_file(path),
        size_bytes=path.stat().st_size,
        row_counts=row_counts,
        feed_start_date=info.get("feed_start_date") or None,
        feed_end_date=info.get("feed_end_date") or None,
        feed_version=info.get("feed_version") or None,
    )
