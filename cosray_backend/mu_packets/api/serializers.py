from __future__ import annotations

import logging
import re
from typing import Any

from rest_framework import serializers

from cosray_backend.mu_packets.services import parse_timestamp

MEASUREMENT_NAME_PATTERN = re.compile(r"^[A-Za-z0-9_.:]+(?:-[A-Za-z0-9_.:]+)?$")
DEVICE_PATTERN = re.compile(r"^[A-Za-z0-9_:]+(?:-[A-Za-z0-9_:]+)?(?:\.[A-Za-z0-9_:]+(?:-[A-Za-z0-9_:]+)?)*$")

logger = logging.getLogger(__name__)


class PacketRecordSerializer(serializers.Serializer):
    timestamp = serializers.JSONField()
    measurements = serializers.DictField(child=serializers.JSONField())

    def validate_timestamp(self, value: Any) -> int:
        try:
            return parse_timestamp(value)
        except ValueError as exc:  # pragma: no cover - exercised through API tests
            logger.exception("Invalid timestamp encountered during validation.")
            raise serializers.ValidationError("Invalid timestamp format.") from exc

    def validate_measurements(self, value: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(value, dict) or not value:
            raise serializers.ValidationError("measurements must contain at least one entry")

        validated: dict[str, Any] = {}
        for name, raw in value.items():
            if not isinstance(name, str) or not name:
                raise serializers.ValidationError("measurement names must be non-empty strings")
            if not MEASUREMENT_NAME_PATTERN.fullmatch(name):
                raise serializers.ValidationError(f"Invalid measurement name: {name}")
            if isinstance(raw, (list, dict)):
                raise serializers.ValidationError("measurement values must be scalar types")
            if raw is None:
                raise serializers.ValidationError("measurement values cannot be null")
            validated[name] = raw
        return validated


class PacketSerializer(serializers.Serializer):
    device = serializers.CharField()
    records = PacketRecordSerializer(many=True)

    def validate_device(self, value: str) -> str:
        device = value.strip()
        if not device:
            raise serializers.ValidationError("device cannot be blank")
        if not DEVICE_PATTERN.match(device):
            raise serializers.ValidationError("device contains invalid characters")
        return device

    def validate_records(self, value: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not value:
            raise serializers.ValidationError("records must not be empty")
        return value


__all__ = ["PacketSerializer", "PacketRecordSerializer"]
