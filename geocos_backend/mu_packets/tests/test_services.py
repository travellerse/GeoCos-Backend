from __future__ import annotations

import pytest

from geocos_backend.mu_packets.services import normalize_device_path, parse_timestamp


def test_parse_timestamp_handles_iso8601() -> None:
    assert parse_timestamp("2024-01-01T00:00:00Z") == 1_704_067_200_000


def test_parse_timestamp_rejects_boolean() -> None:
    with pytest.raises(ValueError):
        parse_timestamp(True)


@pytest.mark.parametrize(
    ("device", "expected"),
    [
        ("factory.unit1", "root.geocos.factory.unit1"),
        ("root.geocos.device2", "root.geocos.device2"),
        (".nested.device", "root.geocos.nested.device"),
    ],
)
def test_normalize_device_path(device: str, expected: str) -> None:
    assert normalize_device_path(device) == expected


def test_normalize_device_path_rejects_blank() -> None:
    with pytest.raises(ValueError):
        normalize_device_path("   ")
