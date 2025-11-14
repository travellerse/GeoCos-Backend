from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence

from cosray_backend.iotdb.client import TimeSeriesRecord

from .timeutils import parse_timestamp

TRIPLET_LENGTH = 3


def _require_mapping(value: object, field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"Field '{field_name}' must be a mapping")
    return value


def _require_sequence(value: object, field_name: str) -> Sequence[Mapping[str, Any]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise ValueError(f"Field '{field_name}' must be a sequence of mappings")
    if not value:
        raise ValueError(f"Field '{field_name}' must not be empty")
    if not all(isinstance(item, Mapping) for item in value):
        raise ValueError(f"All items in '{field_name}' must be mappings")
    return value  # type: ignore[return-value]


def _require_field(payload: Mapping[str, Any], field_name: str) -> Any:
    if field_name not in payload:
        raise ValueError(f"Missing required field '{field_name}'")
    return payload[field_name]


def _coerce_int(value: object, field_name: str, *, allow_negative: bool = True) -> int:
    if isinstance(value, bool):
        raise ValueError(f"Field '{field_name}' must be an integer")
    try:
        coerced = int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Field '{field_name}' must be an integer") from exc
    if not allow_negative and coerced < 0:
        raise ValueError(f"Field '{field_name}' must be non-negative")
    return coerced


def _coerce_triplet(value: object, field_name: str) -> tuple[int, int, int] | None:
    if value is None:
        return None
    if isinstance(value, (bytes, bytearray)):
        if len(value) != TRIPLET_LENGTH:
            raise ValueError(f"Field '{field_name}' must contain exactly three bytes")
        return tuple(int(b) & 0xFF for b in value)
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        if len(value) != TRIPLET_LENGTH:
            raise ValueError(f"Field '{field_name}' must contain exactly three elements")
        return tuple(
            _coerce_int(part, f"{field_name}[{index}]", allow_negative=False) & 0xFF
            for index, part in enumerate(value)
        )
    raise ValueError(f"Field '{field_name}' must be a sequence of three integers")


def _coerce_optional_bytes(value: object, field_name: str) -> tuple[int, ...] | None:
    if value is None:
        return None
    if isinstance(value, (bytes, bytearray)):
        return tuple(int(b) & 0xFF for b in value)
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return tuple(_coerce_int(part, field_name, allow_negative=False) & 0xFF for part in value)
    raise ValueError(f"Field '{field_name}' must be a byte sequence")


def _format_triplet(triplet: tuple[int, int, int]) -> str:
    return "-".join(f"{byte:02X}" for byte in triplet)


def _format_bytes(values: Iterable[int]) -> str:
    return "-".join(f"{value:02X}" for value in values)


@dataclass(frozen=True)
class MuonEvent:
    cpu_time: int
    energy: int
    pps: int
    timestamp_ms: int | None = None

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "MuonEvent":
        mapping = _require_mapping(payload, "muon_event")
        cpu_time = _coerce_int(_require_field(mapping, "cpu_time"), "cpu_time", allow_negative=False)
        energy = _coerce_int(_require_field(mapping, "energy"), "energy", allow_negative=False)
        pps = _coerce_int(_require_field(mapping, "pps"), "pps", allow_negative=False)
        timestamp_value = mapping.get("timestamp")
        timestamp_ms = parse_timestamp(timestamp_value) if timestamp_value is not None else None
        return cls(cpu_time=cpu_time, energy=energy, pps=pps, timestamp_ms=timestamp_ms)


@dataclass(frozen=True)
class MuonPacket:
    package_counter: int
    utc_ms: int
    events: tuple[MuonEvent, ...]
    header: tuple[int, int, int] | None = None
    tail: tuple[int, int, int] | None = None
    crc: int | None = None
    reserved: tuple[int, ...] | None = None

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "MuonPacket":
        mapping = _require_mapping(payload, "muon_packet")
        package_counter = _coerce_int(
            _require_field(mapping, "package_counter"), "package_counter", allow_negative=False
        )
        utc_ms = parse_timestamp(_require_field(mapping, "utc"))
        events_payload = _require_sequence(_require_field(mapping, "events"), "events")
        events = tuple(MuonEvent.from_dict(item) for item in events_payload)
        header = _coerce_triplet(mapping.get("head"), "head")
        tail = _coerce_triplet(mapping.get("tail"), "tail")
        crc = mapping.get("crc")
        crc_value = _coerce_int(crc, "crc", allow_negative=False) if crc is not None else None
        reserved = _coerce_optional_bytes(mapping.get("reserved"), "reserved")
        return cls(
            package_counter=package_counter,
            utc_ms=utc_ms,
            events=events,
            header=header,
            tail=tail,
            crc=crc_value,
            reserved=reserved,
        )

    def to_time_series_records(self) -> list[TimeSeriesRecord]:
        if not self.events:
            raise ValueError("MuonPacket must contain at least one event")

        records: list[TimeSeriesRecord] = []
        event_count = len(self.events)

        for index, event in enumerate(self.events):
            timestamp = event.timestamp_ms if event.timestamp_ms is not None else self.utc_ms + index
            measurements: dict[str, Any] = {
                "muon.energy": event.energy,
                "muon.pps": event.pps,
                "muon.cpu_time": event.cpu_time,
                "muon.package_counter": self.package_counter,
                "muon.event_index": index,
                "muon.event_count": event_count,
                "muon.utc_ms": self.utc_ms,
            }

            if self.crc is not None:
                measurements["muon.packet_crc"] = self.crc
            if self.header is not None:
                measurements["muon.packet_header"] = _format_triplet(self.header)
            if self.tail is not None:
                measurements["muon.packet_tail"] = _format_triplet(self.tail)
            if self.reserved is not None:
                measurements["muon.packet_reserved"] = _format_bytes(self.reserved)

            records.append(TimeSeriesRecord(timestamp=timestamp, measurements=measurements))

        return records


@dataclass(frozen=True)
class TimelineEvent:
    cpu_time: int
    pps: int
    utc_ms: int
    pps_utc: int
    cputime_pps: int
    gps_long: int
    gps_lat: int
    gps_alt: int
    acc_x: int
    acc_y: int
    acc_z: int
    sipm_temperature: int
    mcu_temperature: int
    sipm_current: int
    sipm_voltage: int
    timestamp_ms: int | None = None

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "TimelineEvent":
        mapping = _require_mapping(payload, "timeline_event")
        timestamp_value = mapping.get("timestamp")
        timestamp_ms = parse_timestamp(timestamp_value) if timestamp_value is not None else None
        utc_ms = parse_timestamp(_require_field(mapping, "utc"))

        return cls(
            cpu_time=_coerce_int(_require_field(mapping, "cpu_time"), "cpu_time", allow_negative=False),
            pps=_coerce_int(_require_field(mapping, "pps"), "pps", allow_negative=False),
            utc_ms=utc_ms,
            pps_utc=_coerce_int(_require_field(mapping, "pps_utc"), "pps_utc", allow_negative=False),
            cputime_pps=_coerce_int(_require_field(mapping, "cputime_pps"), "cputime_pps", allow_negative=False),
            gps_long=_coerce_int(_require_field(mapping, "gps_long"), "gps_long"),
            gps_lat=_coerce_int(_require_field(mapping, "gps_lat"), "gps_lat"),
            gps_alt=_coerce_int(_require_field(mapping, "gps_alt"), "gps_alt"),
            acc_x=_coerce_int(_require_field(mapping, "acc_x"), "acc_x"),
            acc_y=_coerce_int(_require_field(mapping, "acc_y"), "acc_y"),
            acc_z=_coerce_int(_require_field(mapping, "acc_z"), "acc_z"),
            sipm_temperature=_coerce_int(_require_field(mapping, "SiPMTmp"), "SiPMTmp", allow_negative=False),
            mcu_temperature=_coerce_int(_require_field(mapping, "MCUTmp"), "MCUTmp", allow_negative=False),
            sipm_current=_coerce_int(_require_field(mapping, "SiPMImon"), "SiPMImon", allow_negative=False),
            sipm_voltage=_coerce_int(_require_field(mapping, "SiPMVmon"), "SiPMVmon", allow_negative=False),
            timestamp_ms=timestamp_ms,
        )


@dataclass(frozen=True)
class TimelinePacket:
    package_counter: int
    events: tuple[TimelineEvent, ...]
    header: tuple[int, int, int] | None = None
    tail: tuple[int, int, int] | None = None
    crc: int | None = None
    reserved: tuple[int, ...] | None = None

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "TimelinePacket":
        mapping = _require_mapping(payload, "timeline_packet")
        package_counter = _coerce_int(
            _require_field(mapping, "package_counter"), "package_counter", allow_negative=False
        )
        events_payload = _require_sequence(_require_field(mapping, "events"), "events")
        events = tuple(TimelineEvent.from_dict(item) for item in events_payload)
        header = _coerce_triplet(mapping.get("head"), "head")
        tail = _coerce_triplet(mapping.get("tail"), "tail")
        crc = mapping.get("crc")
        crc_value = _coerce_int(crc, "crc", allow_negative=False) if crc is not None else None
        reserved = _coerce_optional_bytes(mapping.get("reserved"), "reserved")
        return cls(
            package_counter=package_counter,
            events=events,
            header=header,
            tail=tail,
            crc=crc_value,
            reserved=reserved,
        )

    def to_time_series_records(self) -> list[TimeSeriesRecord]:
        if not self.events:
            raise ValueError("TimelinePacket must contain at least one event")

        records: list[TimeSeriesRecord] = []
        event_count = len(self.events)

        # Check if all events are missing both timestamp_ms and utc_ms
        if all(event.timestamp_ms is None and event.utc_ms is None for event in self.events):
            raise ValueError("All events are missing both timestamp_ms and utc_ms; cannot infer timestamps reliably.")

        for index, event in enumerate(self.events):
            timestamp = event.timestamp_ms or event.utc_ms or (self.events[0].utc_ms + index)
            measurements: dict[str, Any] = {
                "timeline.cpu_time": event.cpu_time,
                "timeline.pps": event.pps,
                "timeline.utc_ms": event.utc_ms,
                "timeline.pps_utc": event.pps_utc,
                "timeline.cputime_pps": event.cputime_pps,
                "timeline.gps_long": event.gps_long,
                "timeline.gps_lat": event.gps_lat,
                "timeline.gps_alt": event.gps_alt,
                "timeline.acc_x": event.acc_x,
                "timeline.acc_y": event.acc_y,
                "timeline.acc_z": event.acc_z,
                "timeline.sipm_temperature": event.sipm_temperature,
                "timeline.mcu_temperature": event.mcu_temperature,
                "timeline.sipm_current": event.sipm_current,
                "timeline.sipm_voltage": event.sipm_voltage,
                "timeline.package_counter": self.package_counter,
                "timeline.event_index": index,
                "timeline.event_count": event_count,
            }

            if self.crc is not None:
                measurements["timeline.packet_crc"] = self.crc
            if self.header is not None:
                measurements["timeline.packet_header"] = _format_triplet(self.header)
            if self.tail is not None:
                measurements["timeline.packet_tail"] = _format_triplet(self.tail)
            if self.reserved is not None:
                measurements["timeline.packet_reserved"] = _format_bytes(self.reserved)

            records.append(TimeSeriesRecord(timestamp=timestamp, measurements=measurements))

        return records


__all__ = [
    "MuonEvent",
    "MuonPacket",
    "TimelineEvent",
    "TimelinePacket",
]
