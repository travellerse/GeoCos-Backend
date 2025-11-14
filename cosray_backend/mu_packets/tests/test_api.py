from __future__ import annotations

import pytest
from rest_framework.test import APIClient

from cosray_backend.iotdb.client import TimeSeriesRecord
from cosray_backend.iotdb.exceptions import IoTDBWriteError
from cosray_backend.mu_packets.domain import MuonPacket, TimelinePacket
from cosray_backend.users.tests.factories import UserFactory

pytestmark = pytest.mark.django_db


@pytest.fixture
def api_client() -> APIClient:
    return APIClient()


def test_create_packet_success(api_client: APIClient, monkeypatch: pytest.MonkeyPatch) -> None:
    user = UserFactory()
    api_client.force_authenticate(user=user)

    captured: dict[str, object] = {}

    def fake_ingest(device: str, records: list[TimeSeriesRecord]) -> None:
        captured["device"] = device
        captured["records"] = records

    monkeypatch.setattr("cosray_backend.mu_packets.api.views.ingest_packet", fake_ingest)

    payload = {
        "device": "factory.unit1",
        "records": [
            {
                "timestamp": 1_700_000_000_000,
                "measurements": {
                    "temperature": 20.5,
                    "status": True,
                },
            }
        ],
    }

    response = api_client.post("/api/mu-packets/", data=payload, format="json")

    assert response.status_code == 201
    assert response.data == {
        "device": "root.cosray.factory.unit1",
        "packet_type": "timeseries",
        "records_written": 1,
    }
    assert captured["device"] == "factory.unit1"
    written_records = captured["records"]
    assert isinstance(written_records, list)
    assert len(written_records) == 1
    written_record = written_records[0]
    assert isinstance(written_record, TimeSeriesRecord)
    assert written_record.timestamp == 1_700_000_000_000
    assert written_record.measurements["temperature"] == pytest.approx(20.5)
    assert written_record.measurements["status"] is True


def test_create_packet_iotdb_write_failure(api_client: APIClient, monkeypatch: pytest.MonkeyPatch) -> None:
    user = UserFactory()
    api_client.force_authenticate(user=user)

    def failing_ingest(device: str, records: list[TimeSeriesRecord]) -> None:
        raise IoTDBWriteError("boom")

    monkeypatch.setattr("cosray_backend.mu_packets.api.views.ingest_packet", failing_ingest)

    payload = {
        "device": "factory.unit2",
        "records": [
            {
                "timestamp": 1,
                "measurements": {"temperature": 18.2},
            }
        ],
    }

    response = api_client.post("/api/mu-packets/", data=payload, format="json")

    assert response.status_code == 503
    assert response.data == {"detail": "Failed to write data to IoTDB"}


def test_create_packet_validation_error(api_client: APIClient) -> None:
    user = UserFactory()
    api_client.force_authenticate(user=user)

    payload = {
        "device": " ",
        "records": [],
    }

    response = api_client.post("/api/mu-packets/", data=payload, format="json")

    assert response.status_code == 400
    assert "device" in response.data


def test_create_packet_invalid_measurement_name(api_client: APIClient) -> None:
    user = UserFactory()
    api_client.force_authenticate(user=user)

    payload = {
        "device": "test-device",
        "records": [
            {
                "timestamp": "2024-06-01T12:00:00Z",
                "measurements": {
                    "invalid name!": 42,  # illegal characters in measurement name
                },
            }
        ],
    }

    response = api_client.post("/api/mu-packets/", data=payload, format="json")

    assert response.status_code == 400
    assert "measurements" in response.data["records"][0]


def test_create_packet_invalid_measurement_value(api_client: APIClient) -> None:
    user = UserFactory()
    api_client.force_authenticate(user=user)

    payload = {
        "device": "test-device",
        "records": [
            {
                "timestamp": "2024-06-01T12:00:00Z",
                "measurements": {
                    "valid_name": {"not": "a number"},  # unsupported type
                },
            }
        ],
    }

    response = api_client.post("/api/mu-packets/", data=payload, format="json")

    assert response.status_code == 400
    assert "measurements" in response.data["records"][0]


def test_create_packet_muon_success(api_client: APIClient, monkeypatch: pytest.MonkeyPatch) -> None:
    user = UserFactory()
    api_client.force_authenticate(user=user)

    captured: dict[str, object] = {}

    def fake_ingest(device: str, packet: MuonPacket) -> int:
        captured["device"] = device
        captured["packet"] = packet
        return len(packet.events)

    monkeypatch.setattr("cosray_backend.mu_packets.api.views.ingest_muon_packet", fake_ingest)

    payload = {
        "device": "factory.unit3",
        "packet_type": "muon",
        "muon_packet": {
            "package_counter": 5,
            "utc": 1_704_067_200,
            "events": [
                {"cpu_time": 1, "energy": 100, "pps": 2, "timestamp": 1_704_067_200_250},
                {"cpu_time": 2, "energy": 150, "pps": 3},
            ],
            "head": [0xAA, 0xBB, 0xCC],
        },
    }

    response = api_client.post("/api/mu-packets/", data=payload, format="json")

    assert response.status_code == 201
    assert response.data == {
        "device": "root.cosray.factory.unit3",
        "packet_type": "muon",
        "records_written": 2,
    }

    assert captured["device"] == "factory.unit3"
    packet = captured["packet"]
    assert isinstance(packet, MuonPacket)
    assert packet.package_counter == 5
    assert packet.utc_ms == 1_704_067_200_000
    assert len(packet.events) == 2


def test_create_packet_timeline_success(api_client: APIClient, monkeypatch: pytest.MonkeyPatch) -> None:
    user = UserFactory()
    api_client.force_authenticate(user=user)

    captured: dict[str, object] = {}

    def fake_ingest(device: str, packet: TimelinePacket) -> int:
        captured["device"] = device
        captured["packet"] = packet
        return len(packet.events)

    monkeypatch.setattr("cosray_backend.mu_packets.api.views.ingest_timeline_packet", fake_ingest)

    payload = {
        "device": "factory.unit4",
        "packet_type": "timeline",
        "timeline_packet": {
            "package_counter": 9,
            "events": [
                {
                    "cpu_time": 1000,
                    "pps": 10,
                    "utc": 1_704_067_210,
                    "pps_utc": 10,
                    "cputime_pps": 500,
                    "gps_long": 100_000,
                    "gps_lat": -50_000,
                    "gps_alt": 500,
                    "acc_x": 1,
                    "acc_y": 0,
                    "acc_z": -1,
                    "SiPMTmp": 400,
                    "MCUTmp": 45,
                    "SiPMImon": 200,
                    "SiPMVmon": 350,
                }
            ],
        },
    }

    response = api_client.post("/api/mu-packets/", data=payload, format="json")

    assert response.status_code == 201
    assert response.data == {
        "device": "root.cosray.factory.unit4",
        "packet_type": "timeline",
        "records_written": 1,
    }

    assert captured["device"] == "factory.unit4"
    packet = captured["packet"]
    assert isinstance(packet, TimelinePacket)
    assert packet.package_counter == 9
    assert len(packet.events) == 1
