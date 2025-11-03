from __future__ import annotations

import logging
from contextlib import contextmanager
from dataclasses import dataclass
from decimal import Decimal
from functools import lru_cache
from typing import Any, Iterator, Mapping, Sequence

from django.conf import settings
from iotdb.Session import Session
from iotdb.SessionPool import PoolConfig, SessionPool
from iotdb.utils.IoTDBConstants import TSDataType

from .exceptions import IoTDBConfigurationError, IoTDBWriteError

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TimeSeriesRecord:
    """Represents a single IoTDB record for one device."""

    timestamp: int
    measurements: Mapping[str, Any]


@dataclass(frozen=True)
class IoTDBSettings:
    host: str
    port: int
    username: str
    password: str
    fetch_size: int
    zone_id: str
    max_retry: int
    pool_size: int
    pool_wait_timeout_ms: int
    use_ssl: bool
    ca_certs: str | None
    node_urls: tuple[str, ...]
    enable_redirection: bool

    @classmethod
    def from_django(cls) -> "IoTDBSettings":
        config = getattr(settings, "IOTDB", None)
        if not config:
            raise IoTDBConfigurationError("IOTDB settings are missing. Define settings.IOTDB.")

        ca_certs_raw = config.get("CA_CERTS") or None
        node_urls_raw = config.get("NODE_URLS") or ()

        return cls(
            host=config.get("HOST", "127.0.0.1"),
            port=int(config.get("PORT", 6667)),
            username=config.get("USERNAME", "root"),
            password=config.get("PASSWORD", "root"),
            fetch_size=int(config.get("FETCH_SIZE", 1024)),
            zone_id=config.get("ZONE_ID", "UTC+8"),
            max_retry=int(config.get("MAX_RETRY", 3)),
            pool_size=int(config.get("POOL_SIZE", 5)),
            pool_wait_timeout_ms=int(config.get("POOL_WAIT_TIMEOUT_MS", 3000)),
            use_ssl=bool(config.get("USE_SSL", False)),
            ca_certs=ca_certs_raw,
            node_urls=tuple(node_urls_raw),
            enable_redirection=bool(config.get("ENABLE_REDIRECTION", True)),
        )


class IoTDBSessionManager:
    """Manages IoTDB session pool lifecycle."""

    def __init__(self, settings_obj: IoTDBSettings):
        self._settings = settings_obj
        self._pool: SessionPool | None = None

    def _ensure_pool(self) -> None:
        if self._pool is None:
            self._pool = self._create_pool()

    def _create_pool(self) -> SessionPool:
        pool_kwargs: dict[str, Any] = {
            "user_name": self._settings.username,
            "password": self._settings.password,
            "fetch_size": self._settings.fetch_size,
            "time_zone": self._settings.zone_id,
            "max_retry": self._settings.max_retry,
            "enable_redirection": self._settings.enable_redirection,
        }

        if self._settings.node_urls:
            pool_kwargs["node_urls"] = list(self._settings.node_urls)
        else:
            pool_kwargs["host"] = self._settings.host
            pool_kwargs["port"] = self._settings.port

        if self._settings.use_ssl:
            pool_kwargs["use_ssl"] = True
            if self._settings.ca_certs:
                pool_kwargs["ca_certs"] = self._settings.ca_certs

        pool_config = PoolConfig(**pool_kwargs)
        return SessionPool(pool_config, self._settings.pool_size, self._settings.pool_wait_timeout_ms)

    @contextmanager
    def acquire(self) -> Iterator[Session]:
        self._ensure_pool()
        if self._pool is None:
            raise IoTDBConfigurationError("Session pool was not initialized.")

        session = self._pool.get_session()
        try:
            yield session
        finally:
            self._pool.put_back(session)

    def close(self) -> None:
        if self._pool is not None:
            self._pool.close()
            self._pool = None


class IoTDBService:
    """High-level helper for inserting records into IoTDB."""

    def __init__(self, session_manager: IoTDBSessionManager):
        self._sessions = session_manager

    def write_records(self, device: str, records: Sequence[TimeSeriesRecord]) -> None:
        if not records:
            raise ValueError("records must contain at least one element")

        time_list: list[int] = []
        measurements_list: list[list[str]] = []
        data_types_list: list[list[TSDataType]] = []
        values_list: list[list[Any]] = []

        for record in records:
            if not record.measurements:
                raise ValueError("Each record must provide at least one measurement")

            measurements: list[str] = []
            row_values: list[Any] = []
            row_types: list[TSDataType] = []

            for name, raw_value in record.measurements.items():
                if not isinstance(name, str) or not name:
                    raise ValueError("Measurement names must be non-empty strings")

                coerced_value, data_type = self._coerce_value(raw_value)
                measurements.append(name)
                row_values.append(coerced_value)
                row_types.append(data_type)

            time_list.append(int(record.timestamp))
            measurements_list.append(measurements)
            data_types_list.append(row_types)
            values_list.append(row_values)

        try:
            with self._sessions.acquire() as session:
                session.insert_records_of_one_device(
                    device,
                    time_list,
                    measurements_list,
                    data_types_list,
                    values_list,
                )
        except Exception as exc:  # noqa: BLE001 - surface IoTDB errors uniformly
            logger.exception("Failed to write %s records to IoTDB for device %s", len(records), device)
            raise IoTDBWriteError("Failed to write records to IoTDB") from exc

    @staticmethod
    def _coerce_value(value: Any) -> tuple[Any, TSDataType]:
        if isinstance(value, bool):
            return value, TSDataType.BOOLEAN
        if isinstance(value, int) and not isinstance(value, bool):
            return value, TSDataType.INT64
        if isinstance(value, float):
            return value, TSDataType.DOUBLE
        if isinstance(value, Decimal):
            return float(value), TSDataType.DOUBLE
        if isinstance(value, str):
            return value, TSDataType.TEXT
        raise TypeError(f"Unsupported measurement value type: {type(value)!r}")


@lru_cache(maxsize=1)
def get_iotdb_service() -> IoTDBService:
    settings_obj = IoTDBSettings.from_django()
    manager = IoTDBSessionManager(settings_obj)
    return IoTDBService(manager)


def reset_iotdb_service_cache() -> None:
    get_iotdb_service.cache_clear()


__all__ = [
    "IoTDBService",
    "IoTDBSessionManager",
    "IoTDBSettings",
    "TimeSeriesRecord",
    "get_iotdb_service",
    "reset_iotdb_service_cache",
]
