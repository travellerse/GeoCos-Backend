from __future__ import annotations

import logging
from typing import Any

from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.viewsets import GenericViewSet

from geocos_backend.iotdb.client import TimeSeriesRecord
from geocos_backend.iotdb.exceptions import IoTDBError, IoTDBWriteError
from geocos_backend.mu_packets.api.serializers import PacketSerializer
from geocos_backend.mu_packets.services import ingest_packet, normalize_device_path

logger = logging.getLogger(__name__)


class PacketViewSet(GenericViewSet):
    permission_classes = [IsAuthenticated]
    serializer_class = PacketSerializer
    http_method_names = ["post"]

    def create(self, request, *args: Any, **kwargs: Any) -> Response:
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        validated = serializer.validated_data
        records = [
            TimeSeriesRecord(timestamp=record["timestamp"], measurements=record["measurements"])
            for record in validated["records"]
        ]

        try:
            ingest_packet(validated["device"], records)
            normalized_device = normalize_device_path(validated["device"])
        except IoTDBWriteError as exc:
            logger.exception("IoTDB write failed for device %s", validated["device"])
            return Response(
                {"detail": "Failed to write data to IoTDB"},
                status=status.HTTP_503_SERVICE_UNAVAILABLE,
            )
        except (IoTDBError, ValueError) as exc:
            logger.exception("IoTDB integration error for device %s", validated["device"])
            return Response({"detail": str(exc)}, status=status.HTTP_400_BAD_REQUEST)

        return Response(
            {"device": normalized_device, "records_written": len(records)},
            status=status.HTTP_201_CREATED,
        )


__all__ = ["PacketViewSet"]
