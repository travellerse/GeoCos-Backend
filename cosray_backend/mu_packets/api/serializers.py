from __future__ import annotations

import logging
import re
from typing import Any

from rest_framework import serializers

from cosray_backend.mu_packets.services import parse_timestamp

MEASUREMENT_NAME_PATTERN = re.compile(r"^[A-Za-z0-9_.:]+(?:-[A-Za-z0-9_.:]+)?$")
DEVICE_PATTERN = re.compile(r"^[A-Za-z0-9_:]+(?:-[A-Za-z0-9_:]+)?(?:\.[A-Za-z0-9_:]+(?:-[A-Za-z0-9_:]+)?)*$")

logger = logging.getLogger(__name__)


class TimestampField(serializers.JSONField):
    def to_internal_value(self, value: Any) -> int | None:
        if value is None:
            if self.allow_null:
                return None
            raise serializers.ValidationError("Timestamp value cannot be null")

        try:
            return parse_timestamp(value)
        except ValueError as exc:  # pragma: no cover - exercised through API tests
            logger.exception("Invalid timestamp encountered during validation.")
            raise serializers.ValidationError("Invalid timestamp format.") from exc


class ByteArrayField(serializers.ListField):
    def __init__(self, *, min_length: int | None = None, max_length: int | None = None, **kwargs: Any) -> None:
        super().__init__(
            child=serializers.IntegerField(min_value=0, max_value=255),
            min_length=min_length,
            max_length=max_length,
            **kwargs,
        )


class ByteTripletField(ByteArrayField):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(min_length=3, max_length=3, **kwargs)


class PacketRecordSerializer(serializers.Serializer):
    timestamp = TimestampField()
    measurements = serializers.DictField(child=serializers.JSONField())

    def validate_measurements(self, value: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(value, dict) or not value:
            raise serializers.ValidationError("measurements must contain at least one entry")

        validated: dict[str, Any] = {}
        for name, raw in value.items():
            if not isinstance(name, str) or not name:
                raise serializers.ValidationError("measurement names must be non-empty strings")
            if not MEASUREMENT_NAME_PATTERN.fullmatch(name):
                raise serializers.ValidationError(f"Invalid measurement name: {name}")
            if isinstance(raw, (list, dict, bytes, bytearray)):
                raise serializers.ValidationError(
                    "measurement values must be scalar types and cannot be bytes or bytearray"
                )
            if raw is None:
                raise serializers.ValidationError("measurement values cannot be null")
            validated[name] = raw
        return validated


class MuonEventSerializer(serializers.Serializer):
    cpu_time = serializers.IntegerField(min_value=0)
    energy = serializers.IntegerField(min_value=0)
    pps = serializers.IntegerField(min_value=0)
    timestamp = TimestampField(required=False, allow_null=True)


class MuonPacketSerializer(serializers.Serializer):
    package_counter = serializers.IntegerField(min_value=0)
    utc = TimestampField()
    events = MuonEventSerializer(many=True)
    head = ByteTripletField(required=False)
    tail = ByteTripletField(required=False)
    crc = serializers.IntegerField(min_value=0, required=False)
    reserved = ByteArrayField(required=False, allow_empty=True)

    def validate_events(self, value: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not value:
            raise serializers.ValidationError("events must contain at least one entry")
        return value


class TimelineEventSerializer(serializers.Serializer):
    cpu_time = serializers.IntegerField(min_value=0)
    pps = serializers.IntegerField(min_value=0)
    utc = TimestampField()
    pps_utc = serializers.IntegerField(min_value=0)
    cputime_pps = serializers.IntegerField(min_value=0)
    gps_long = serializers.IntegerField()
    gps_lat = serializers.IntegerField(min_value=-2_147_483_648, max_value=2_147_483_647)
    gps_alt = serializers.IntegerField(min_value=-32_768, max_value=32_767)
    acc_x = serializers.IntegerField(min_value=-128, max_value=127)
    acc_y = serializers.IntegerField(min_value=-128, max_value=127)
    acc_z = serializers.IntegerField(min_value=-128, max_value=127)
    SiPMTmp = serializers.IntegerField(min_value=0, max_value=65_535)
    MCUTmp = serializers.IntegerField(min_value=0, max_value=255)
    SiPMImon = serializers.IntegerField(min_value=0, max_value=65_535)
    SiPMVmon = serializers.IntegerField(min_value=0, max_value=65_535)
    timestamp = TimestampField(required=False, allow_null=True)


class TimelinePacketSerializer(serializers.Serializer):
    package_counter = serializers.IntegerField(min_value=0)
    events = TimelineEventSerializer(many=True)
    head = ByteTripletField(required=False)
    tail = ByteTripletField(required=False)
    crc = serializers.IntegerField(min_value=0, required=False)
    reserved = ByteArrayField(required=False, allow_empty=True)

    def validate_events(self, value: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not value:
            raise serializers.ValidationError("events must contain at least one entry")
        return value


class PacketSerializer(serializers.Serializer):
    device = serializers.CharField()
    packet_type = serializers.ChoiceField(choices=("timeseries", "muon", "timeline"), default="timeseries")
    records = PacketRecordSerializer(many=True, required=False)
    muon_packet = MuonPacketSerializer(required=False)
    timeline_packet = TimelinePacketSerializer(required=False)

    def validate_device(self, value: str) -> str:
        device = value.strip()
        if not device:
            raise serializers.ValidationError("device cannot be blank")
        if not DEVICE_PATTERN.match(device):
            raise serializers.ValidationError("device contains invalid characters")
        return device

    def validate(self, attrs: dict[str, Any]) -> dict[str, Any]:
        packet_type = attrs.get("packet_type", "timeseries")

        if packet_type == "timeseries":
            records = attrs.get("records")
            if not records:
                raise serializers.ValidationError({"records": "records must not be empty"})
        elif packet_type == "muon":
            if "muon_packet" not in attrs or attrs["muon_packet"] is None:
                raise serializers.ValidationError({"muon_packet": "muon_packet is required"})
        elif packet_type == "timeline":
            if "timeline_packet" not in attrs or attrs["timeline_packet"] is None:
                raise serializers.ValidationError({"timeline_packet": "timeline_packet is required"})

        return attrs


__all__ = [
    "ByteArrayField",
    "ByteTripletField",
    "MuonEventSerializer",
    "MuonPacketSerializer",
    "PacketRecordSerializer",
    "PacketSerializer",
    "TimelineEventSerializer",
    "TimelinePacketSerializer",
    "TimestampField",
]
