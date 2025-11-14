from __future__ import annotations

from collections.abc import Sequence
from types import SimpleNamespace

import pytest

from cosray_backend.iotdb.client import TimeSeriesRecord
from cosray_backend.mu_packets.domain import (
    MuonEvent,
    MuonPacket,
    TimelineEvent,
    TimelinePacket,
)
from cosray_backend.mu_packets.services import (
    ingest_muon_packet,
    ingest_timeline_packet,
    normalize_device_path,
    parse_timestamp,
)


def test_parse_timestamp_handles_iso8601() -> None:
    assert parse_timestamp("2024-01-01T00:00:00Z") == 1_704_067_200_000


def test_parse_timestamp_converts_seconds_to_ms() -> None:
    assert parse_timestamp(1_704_067_200) == 1_704_067_200_000


def test_parse_timestamp_rejects_boolean() -> None:
    with pytest.raises(ValueError):
        parse_timestamp(True)


def test_parse_timestamp_rejects_list() -> None:
    with pytest.raises(ValueError):
        parse_timestamp([])


def test_parse_timestamp_rejects_dict() -> None:
    with pytest.raises(ValueError):
        parse_timestamp({})


def test_parse_timestamp_rejects_empty_string() -> None:
    with pytest.raises(ValueError):
        parse_timestamp("")


@pytest.mark.parametrize(
    ("device", "expected"),
    [
        ("factory.unit1", "root.cosray.factory.unit1"),
        ("root.cosray.device2", "root.cosray.device2"),
        (".nested.device", "root.cosray.nested.device"),
    ],
)
def test_normalize_device_path(device: str, expected: str) -> None:
    assert normalize_device_path(device) == expected


def test_normalize_device_path_rejects_blank() -> None:
    with pytest.raises(ValueError):
        normalize_device_path("   ")


def test_normalize_device_path_rejects_only_dots() -> None:
    with pytest.raises(ValueError):
        normalize_device_path("...")


def test_normalize_device_path_with_no_root_path_setting(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "cosray_backend.mu_packets.services.settings",
        SimpleNamespace(IOTDB={"ROOT_PATH": ""}),
    )
    assert normalize_device_path("device1") == "device1"
    assert normalize_device_path(".device2") == "device2"


def test_normalize_device_path_table_dialect(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "cosray_backend.mu_packets.services.settings",
        SimpleNamespace(IOTDB={"SQL_DIALECT": "table", "TABLE_NAME_PREFIX": "cosray_"}),
    )

    assert normalize_device_path("detector") == "cosray_detector"
    assert normalize_device_path(" multi /segment ") == "cosray_multi_.segment"
    # Existing prefix should not be duplicated
    assert normalize_device_path("cosray_existing") == "cosray_existing"


def test_ingest_muon_packet(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "cosray_backend.mu_packets.services.settings",
        SimpleNamespace(IOTDB={"ROOT_PATH": "root.cosray"}),
    )

    captured: dict[str, object] = {}

    class DummyService:
        def write_records(self, device: str, records: Sequence[TimeSeriesRecord]) -> None:
            captured["device"] = device
            captured["records"] = list(records)

    monkeypatch.setattr("cosray_backend.mu_packets.services.get_iotdb_service", lambda: DummyService())

    packet = MuonPacket(
        package_counter=42,
        utc_ms=1_704_067_200_000,
        events=(
            MuonEvent(cpu_time=10, energy=512, pps=99, timestamp_ms=None),
            MuonEvent(cpu_time=20, energy=256, pps=100, timestamp_ms=1_704_067_200_500),
        ),
        header=(0xAA, 0xBB, 0xCC),
    )

    count = ingest_muon_packet("factory.detector1", packet)

    assert count == 2
    assert captured["device"] == "root.cosray.factory.detector1"
    records = captured["records"]
    assert isinstance(records, list) and len(records) == 2

    first_record: TimeSeriesRecord = records[0]
    assert first_record.timestamp == packet.utc_ms
    assert first_record.measurements["muon.energy"] == 512
    assert first_record.measurements["muon.package_counter"] == 42
    assert first_record.measurements["muon.packet_header"] == "AA-BB-CC"

    second_record: TimeSeriesRecord = records[1]
    assert second_record.timestamp == 1_704_067_200_500
    assert second_record.measurements["muon.event_index"] == 1


def test_ingest_timeline_packet(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "cosray_backend.mu_packets.services.settings",
        SimpleNamespace(IOTDB={"ROOT_PATH": "root.cosray"}),
    )

    captured: dict[str, object] = {}

    class DummyService:
        def write_records(self, device: str, records: Sequence[TimeSeriesRecord]) -> None:
            captured["device"] = device
            captured["records"] = list(records)

    monkeypatch.setattr("cosray_backend.mu_packets.services.get_iotdb_service", lambda: DummyService())

    event = TimelineEvent(
        cpu_time=100,
        pps=5,
        utc_ms=1_704_067_205_000,
        pps_utc=5,
        cputime_pps=200,
        gps_long=123_456,
        gps_lat=-654_321,
        gps_alt=512,
        acc_x=1,
        acc_y=2,
        acc_z=3,
        sipm_temperature=400,
        mcu_temperature=35,
        sipm_current=500,
        sipm_voltage=600,
        timestamp_ms=None,
    )

    packet = TimelinePacket(
        package_counter=7,
        events=(event,),
        crc=0x1A2B,
    )

    count = ingest_timeline_packet("factory.detector2", packet)

    assert count == 1
    assert captured["device"] == "root.cosray.factory.detector2"
    records = captured["records"]
    assert isinstance(records, list) and len(records) == 1

    record: TimeSeriesRecord = records[0]
    assert record.timestamp == event.utc_ms
    assert record.measurements["timeline.package_counter"] == 7
    assert record.measurements["timeline.packet_crc"] == 0x1A2B
