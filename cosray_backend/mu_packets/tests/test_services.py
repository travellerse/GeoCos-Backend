from __future__ import annotations

import pytest

from cosray_backend.mu_packets.services import normalize_device_path, parse_timestamp


def test_parse_timestamp_handles_iso8601() -> None:
    assert parse_timestamp("2024-01-01T00:00:00Z") == 1_704_067_200_000


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
    monkeypatch.setattr("cosray_backend.mu_packets.services.settings", {"IOTDB": {"ROOT_PATH": ""}})
    assert normalize_device_path("device1") == "device1"
    assert normalize_device_path(".device2") == "device2"
