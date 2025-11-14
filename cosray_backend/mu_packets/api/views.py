from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.viewsets import GenericViewSet

from cosray_backend.iotdb.client import TimeSeriesRecord
from cosray_backend.iotdb.exceptions import IoTDBError, IoTDBWriteError
from cosray_backend.mu_packets.api.serializers import PacketSerializer
from cosray_backend.mu_packets.domain import MuonPacket, TimelinePacket
from cosray_backend.mu_packets.services import (
    ingest_muon_packet,
    ingest_packet,
    ingest_timeline_packet,
    normalize_device_path,
)

logger = logging.getLogger(__name__)


FAILED_WRITE_DETAIL = {"detail": "Failed to write data to IoTDB"}
PROCESSING_ERROR_DETAIL = {"detail": "Error occurred while processing the request"}
INVALID_MUON_DETAIL = {"detail": "Invalid muon packet payload"}
INVALID_TIMELINE_DETAIL = {"detail": "Invalid timeline packet payload"}


class PacketViewSet(GenericViewSet):
    permission_classes = [IsAuthenticated]
    serializer_class = PacketSerializer
    http_method_names = ["post"]

    def create(self, request, *args: Any, **kwargs: Any) -> Response:
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        validated = serializer.validated_data
        packet_type = validated.get("packet_type", "timeseries")
        device = validated["device"]

        write_callable: Callable[[], int]

        if packet_type == "timeseries":
            records = [
                TimeSeriesRecord(timestamp=record["timestamp"], measurements=record["measurements"])
                for record in validated["records"]
            ]

            def write_callable() -> int:
                ingest_packet(device, records)
                return len(records)

        elif packet_type == "muon":
            try:
                muon_packet = MuonPacket.from_dict(validated["muon_packet"])
            except ValueError:
                logger.exception("Invalid muon packet payload for device %s", device)
                return Response(INVALID_MUON_DETAIL, status=status.HTTP_400_BAD_REQUEST)

            def write_callable() -> int:
                return ingest_muon_packet(device, muon_packet)

        elif packet_type == "timeline":
            try:
                timeline_packet = TimelinePacket.from_dict(validated["timeline_packet"])
            except ValueError:
                logger.exception("Invalid timeline packet payload for device %s", device)
                return Response(INVALID_TIMELINE_DETAIL, status=status.HTTP_400_BAD_REQUEST)

            def write_callable() -> int:
                return ingest_timeline_packet(device, timeline_packet)

        else:  # pragma: no cover - defensive programming
            return Response(
                {"detail": f"Unsupported packet_type: {packet_type}"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        try:
            records_written = write_callable()
            normalized_device = normalize_device_path(device)
        except IoTDBWriteError:
            logger.exception("IoTDB write failed for packet type %s on device %s", packet_type, device)
            return Response(FAILED_WRITE_DETAIL, status=status.HTTP_503_SERVICE_UNAVAILABLE)
        except (IoTDBError, ValueError):
            logger.exception("IoTDB integration error for packet type %s on device %s", packet_type, device)
            return Response(PROCESSING_ERROR_DETAIL, status=status.HTTP_400_BAD_REQUEST)

        return Response(
            {
                "device": normalized_device,
                "packet_type": packet_type,
                "records_written": records_written,
            },
            status=status.HTTP_201_CREATED,
        )


__all__ = ["PacketViewSet"]
