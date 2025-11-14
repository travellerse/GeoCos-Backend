from __future__ import annotations

from datetime import timezone as dt_timezone
from typing import Final

from django.utils import timezone
from django.utils.dateparse import parse_datetime

_SECONDS_THRESHOLD: Final[int] = 10**12  # Approximately milliseconds for year 2001


def parse_timestamp(value: object) -> int:
    """Convert various timestamp representations to IoTDB-compatible milliseconds.

    Integer values below :data:`_SECONDS_THRESHOLD` are assumed to be seconds and are
    converted to milliseconds. Float values are truncated to integers. ISO8601 strings
    are parsed via Django utilities. Boolean and container types are rejected.
    """

    if isinstance(value, bool):
        raise ValueError("Boolean values are not valid timestamps")

    if isinstance(value, int):
        return value if value >= _SECONDS_THRESHOLD else value * 1000

    if isinstance(value, float):
        coerced = int(value)
        return coerced if coerced >= _SECONDS_THRESHOLD else coerced * 1000

    if isinstance(value, str):
        candidate = value.strip()
        if not candidate:
            raise ValueError("Timestamp string cannot be empty")

        try:
            numeric = int(candidate)
            return numeric if numeric >= _SECONDS_THRESHOLD else numeric * 1000
        except ValueError:
            pass

        parsed = parse_datetime(candidate)
        if parsed is None:
            raise ValueError(f"Unable to parse timestamp: {value!r}")

        aware_dt = parsed if timezone.is_aware(parsed) else timezone.make_aware(parsed, dt_timezone.utc)
        milliseconds = int(aware_dt.timestamp() * 1000)
        return milliseconds

    raise ValueError(f"Unsupported timestamp type: {type(value)!r}")


__all__ = ["parse_timestamp"]
