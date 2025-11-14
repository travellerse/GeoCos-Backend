from __future__ import annotations

from typing import Any

import pytest

from cosray_backend.mu_packets.domain import (
    MuonEvent,
    MuonPacket,
    TimelineEvent,
    TimelinePacket,
)


def _build_timeline_event(**overrides: Any) -> TimelineEvent:
    defaults = {
        "cpu_time": 1,
        "pps": 1,
        "utc_ms": 0,
        "pps_utc": 1,
        "cputime_pps": 1,
        "gps_long": 1,
        "gps_lat": 2,
        "gps_alt": 3,
        "acc_x": 0,
        "acc_y": 0,
        "acc_z": 0,
        "sipm_temperature": 1,
        "mcu_temperature": 1,
        "sipm_current": 1,
        "sipm_voltage": 1,
        "timestamp_ms": None,
    }
    defaults.update(overrides)
    return TimelineEvent(**defaults)


def test_muon_packet_records_include_metadata() -> None:
    packet = MuonPacket(
        package_counter=99,
        utc_ms=1_700_000_000_000,
        events=(MuonEvent(cpu_time=1, energy=10, pps=5, timestamp_ms=None),),
        header=(0xAA, 0xBB, 0xCC),
        tail=(0x01, 0x02, 0x03),
        crc=0x1A2B,
        reserved=(0x10, 0x20),
    )

    records = packet.to_time_series_records()
    assert len(records) == 1

    fields = records[0].measurements
    assert fields["muon.packet_crc"] == 0x1A2B
    assert fields["muon.packet_header"] == "AA-BB-CC"
    assert fields["muon.packet_tail"] == "01-02-03"
    assert fields["muon.packet_reserved"] == "10-20"


def test_timeline_packet_infers_timestamp_when_first_event_lacks_one() -> None:
    earlier_event = _build_timeline_event(utc_ms=None, timestamp_ms=None)
    intermediate_event = _build_timeline_event(utc_ms=None, timestamp_ms=None)
    reference_event = _build_timeline_event(
        utc_ms=1_700_000_000_000,
        timestamp_ms=1_700_000_000_500,
    )
    packet = TimelinePacket(package_counter=1, events=(earlier_event, intermediate_event, reference_event))

    records = packet.to_time_series_records()
    assert records[0].timestamp == 1_700_000_000_500
    assert records[1].timestamp == 1_700_000_000_501
    assert records[2].timestamp == 1_700_000_000_500


def test_timeline_packet_rejects_missing_timestamps() -> None:
    event_without_time = _build_timeline_event(utc_ms=None, timestamp_ms=None)
    packet = TimelinePacket(package_counter=2, events=(event_without_time, event_without_time))

    with pytest.raises(ValueError, match="missing both timestamp_ms and utc_ms"):
        packet.to_time_series_records()
