from __future__ import annotations

from collections.abc import Sequence

from django.conf import settings

from cosray_backend.iotdb.client import TimeSeriesRecord, get_iotdb_service

from .domain import MuonPacket, TimelinePacket
from .timeutils import parse_timestamp


def normalize_device_path(device: str) -> str:
    config = getattr(settings, "IOTDB", {})
    dialect = str(config.get("SQL_DIALECT", "tree")).strip().lower() or "tree"
    if not (trimmed := device.strip().strip(".")):
        raise ValueError("Device identifier cannot be empty")

    if dialect == "table":
        prefix = config.get("TABLE_NAME_PREFIX") or ""
        sanitized = trimmed.replace("/", ".").replace(" ", "_")
        return f"{prefix}{sanitized}" if prefix and not sanitized.startswith(prefix) else sanitized

    base_path = config.get("ROOT_PATH", "")
    return f"{base_path}.{trimmed}".replace("..", ".") if base_path and not trimmed.startswith(base_path) else trimmed


def ingest_packet(device: str, records: Sequence[TimeSeriesRecord]) -> None:
    normalized_device = normalize_device_path(device)
    _write_records(normalized_device, records)


def ingest_muon_packet(device: str, packet: MuonPacket) -> int:
    normalized_device = normalize_device_path(device)
    records = packet.to_time_series_records()
    return _write_records(normalized_device, records)


def ingest_timeline_packet(device: str, packet: TimelinePacket) -> int:
    normalized_device = normalize_device_path(device)
    records = packet.to_time_series_records()
    return _write_records(normalized_device, records)


def _write_records(normalized_device: str, records: Sequence[TimeSeriesRecord]) -> int:
    if not records:
        raise ValueError("records must contain at least one element")

    service = get_iotdb_service()
    service.write_records(normalized_device, records)
    return len(records)


__all__ = [
    "ingest_packet",
    "ingest_muon_packet",
    "ingest_timeline_packet",
    "normalize_device_path",
    "parse_timestamp",
]
