from __future__ import annotations

from datetime import timezone as dt_timezone
from typing import Sequence

from django.conf import settings
from django.utils import timezone
from django.utils.dateparse import parse_datetime

from cosray_backend.iotdb.client import TimeSeriesRecord, get_iotdb_service


def parse_timestamp(value: object) -> int:
    """Convert various timestamp representations to IoTDB-compatible milliseconds."""

    if isinstance(value, bool):
        raise ValueError("Boolean values are not valid timestamps")

    if isinstance(value, int):
        return value

    if isinstance(value, float):
        return int(value)

    if isinstance(value, str):
        candidate = value.strip()
        if not candidate:
            raise ValueError("Timestamp string cannot be empty")

        try:
            return int(candidate)
        except ValueError:
            pass

        parsed = parse_datetime(candidate)
        if parsed is None:
            raise ValueError(f"Unable to parse timestamp: {value!r}")

        aware_dt = parsed if timezone.is_aware(parsed) else timezone.make_aware(parsed, dt_timezone.utc)
        return int(aware_dt.timestamp() * 1000)

    raise ValueError(f"Unsupported timestamp type: {type(value)!r}")


def normalize_device_path(device: str) -> str:
    base_path = getattr(settings, "IOTDB", {}).get("ROOT_PATH", "")
    trimmed = device.strip().strip(".")
    if not trimmed:
        raise ValueError("Device identifier cannot be empty")

    if base_path and not trimmed.startswith(base_path):
        return f"{base_path}.{trimmed}".replace("..", ".")
    return trimmed


def ingest_packet(device: str, records: Sequence[TimeSeriesRecord]) -> None:
    normalized_device = normalize_device_path(device)
    service = get_iotdb_service()
    service.write_records(normalized_device, records)


__all__ = ["ingest_packet", "normalize_device_path", "parse_timestamp"]
