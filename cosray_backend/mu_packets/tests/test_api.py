from __future__ import annotations

import pytest
from rest_framework.test import APIClient

from cosray_backend.iotdb.client import TimeSeriesRecord
from cosray_backend.iotdb.exceptions import IoTDBWriteError
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
    assert response.data == {"device": "root.cosray.factory.unit1", "records_written": 1}
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
    assert "records" in response.data
