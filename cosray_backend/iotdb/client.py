from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass
from decimal import Decimal
from functools import lru_cache
from typing import Any, Iterator, Literal, Mapping, Sequence

from django.conf import settings
from iotdb.Session import Session
from iotdb.SessionPool import PoolConfig, SessionPool
from iotdb.table_session import TableSession
from iotdb.table_session_pool import TableSessionPool, TableSessionPoolConfig
from iotdb.utils.IoTDBConstants import TSDataType
from iotdb.utils.Tablet import ColumnType, Tablet

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
    enable_compression: bool
    connection_timeout_ms: int | None
    sql_dialect: Literal["tree", "table"]
    database: str | None
    table_name_prefix: str | None

    @classmethod
    def from_django(cls) -> "IoTDBSettings":
        config = getattr(settings, "IOTDB", None)
        if config is None:
            raise IoTDBConfigurationError("IOTDB settings are missing. Define settings.IOTDB.")

        ca_certs_raw = config.get("CA_CERTS") or None
        node_urls_raw = config.get("NODE_URLS") or ()
        sql_dialect = str(config.get("SQL_DIALECT", "tree")).strip().lower() or "tree"
        if sql_dialect not in {"tree", "table"}:
            raise IoTDBConfigurationError(f"Unsupported IoTDB SQL dialect: {sql_dialect!r}")

        connection_timeout_config = config.get("CONNECTION_TIMEOUT_MS")
        connection_timeout_ms: int | None
        if connection_timeout_config in (None, "", 0):
            connection_timeout_ms = None
        else:
            connection_timeout_ms = int(connection_timeout_config)
            if connection_timeout_ms <= 0:
                connection_timeout_ms = None

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
            enable_compression=bool(config.get("ENABLE_COMPRESSION", False)),
            connection_timeout_ms=connection_timeout_ms,
            sql_dialect=sql_dialect,
            database=config.get("DATABASE") or None,
            table_name_prefix=config.get("TABLE_NAME_PREFIX") or None,
        )


class IoTDBSessionManager:
    """Manages IoTDB session pool lifecycle for both tree and table dialects."""

    def __init__(self, settings_obj: IoTDBSettings):
        self._settings = settings_obj
        self._pool: SessionPool | TableSessionPool | None = None

    def _ensure_pool(self) -> None:
        if self._pool is None:
            self._pool = self._create_pool()

    def _create_pool(self) -> SessionPool | TableSessionPool:
        if self._settings.pool_size <= 0:
            raise ValueError(f"pool_size must be positive, got {self._settings.pool_size}")
        if self._settings.pool_wait_timeout_ms <= 0:
            raise ValueError(f"pool_wait_timeout_ms must be positive, got {self._settings.pool_wait_timeout_ms}")

        if self._settings.sql_dialect == "table":
            return self._create_table_pool()
        return self._create_tree_pool()

    def _create_tree_pool(self) -> SessionPool:
        pool_kwargs: dict[str, Any] = {
            "user_name": self._settings.username,
            "password": self._settings.password,
            "fetch_size": self._settings.fetch_size,
            "time_zone": self._settings.zone_id,
            "max_retry": self._settings.max_retry,
            "enable_redirection": self._settings.enable_redirection,
            "enable_compression": self._settings.enable_compression,
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

        if self._settings.connection_timeout_ms is not None:
            pool_kwargs["connection_timeout_in_ms"] = self._settings.connection_timeout_ms

        pool_config = PoolConfig(**pool_kwargs)
        return SessionPool(pool_config, self._settings.pool_size, self._settings.pool_wait_timeout_ms)

    def _create_table_pool(self) -> TableSessionPool:
        node_urls: list[str]
        if self._settings.node_urls:
            node_urls = list(self._settings.node_urls)
        else:
            node_urls = [f"{self._settings.host}:{self._settings.port}"]

        pool_config = TableSessionPoolConfig(
            node_urls=node_urls,
            max_pool_size=self._settings.pool_size,
            username=self._settings.username,
            password=self._settings.password,
            database=self._settings.database,
            fetch_size=self._settings.fetch_size,
            time_zone=self._settings.zone_id,
            enable_redirection=self._settings.enable_redirection,
            enable_compression=self._settings.enable_compression,
            wait_timeout_in_ms=self._settings.pool_wait_timeout_ms,
            max_retry=self._settings.max_retry,
            use_ssl=self._settings.use_ssl,
            ca_certs=self._settings.ca_certs,
            connection_timeout_in_ms=self._settings.connection_timeout_ms,
        )
        return TableSessionPool(pool_config)

    @contextmanager
    def acquire(self) -> Iterator[Session | TableSession]:
        self._ensure_pool()
        if self._pool is None:
            raise IoTDBConfigurationError("Session pool was not initialized.")

        if isinstance(self._pool, TableSessionPool):
            session = self._pool.get_session()
            try:
                yield session
            finally:
                session.close()
        else:
            session = self._pool.get_session()
            try:
                yield session
            finally:
                self._pool.put_back(session)

    def close(self) -> None:
        if self._pool is not None:
            self._pool.close()
            self._pool = None


def _coerce_measurement_value(value: Any) -> tuple[Any, TSDataType]:
    if isinstance(value, bool):
        return value, TSDataType.BOOLEAN
    if isinstance(value, int):
        return value, TSDataType.INT64
    if isinstance(value, float):
        return value, TSDataType.DOUBLE
    if isinstance(value, Decimal):
        return float(value), TSDataType.DOUBLE
    if isinstance(value, str):
        return value, TSDataType.TEXT
    raise TypeError(f"Unsupported measurement value type: {type(value)!r}")


class _BaseWriter(ABC):
    def __init__(self, session_manager: IoTDBSessionManager, settings_obj: IoTDBSettings):
        self._sessions = session_manager
        self._settings = settings_obj

    @abstractmethod
    def write_records(self, target: str, records: Sequence[TimeSeriesRecord]) -> None:
        """Persist the provided records to IoTDB."""


class _TreeIoTDBWriter(_BaseWriter):
    def write_records(self, device: str, records: Sequence[TimeSeriesRecord]) -> None:
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

                coerced_value, data_type = _coerce_measurement_value(raw_value)
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
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "Failed to write %s records to IoTDB for device %s (tree dialect)",
                len(records),
                device,
            )
            raise IoTDBWriteError("Failed to write records to IoTDB") from exc


class _TableIoTDBWriter(_BaseWriter):
    def write_records(self, table_name: str, records: Sequence[TimeSeriesRecord]) -> None:
        column_names, data_types, timestamps, values = self._prepare_tablet_payload(records)
        if not column_names:
            raise ValueError("Records must define at least one measurement for table insertion")

        resolved_table_name = self._resolve_table_name(table_name)
        column_types = ColumnType.FIELD.n_copy(len(column_names))
        tablet = Tablet(resolved_table_name, column_names, data_types, values, timestamps, column_types)

        try:
            with self._sessions.acquire() as session:
                session.insert(tablet)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Failed to write %s records to IoTDB table %s", len(records), resolved_table_name)
            raise IoTDBWriteError("Failed to write records to IoTDB") from exc

    def _prepare_tablet_payload(
        self, records: Sequence[TimeSeriesRecord]
    ) -> tuple[list[str], list[TSDataType], list[int], list[list[Any]]]:
        measurement_order: list[str] = []
        data_types_map: dict[str, TSDataType] = {}
        processed: list[tuple[int, dict[str, Any]]] = []

        for record in records:
            if not record.measurements:
                raise ValueError("Each record must provide at least one measurement")

            row_values: dict[str, Any] = {}
            for name, raw_value in record.measurements.items():
                if not isinstance(name, str) or not name:
                    raise ValueError("Measurement names must be non-empty strings")
                coerced_value, data_type = _coerce_measurement_value(raw_value)
                existing_type = data_types_map.get(name)
                if existing_type is None:
                    data_types_map[name] = data_type
                    measurement_order.append(name)
                elif existing_type != data_type:
                    raise TypeError(
                        f"Measurement '{name}' uses inconsistent data types: {existing_type} vs {data_type}"
                    )
                row_values[name] = coerced_value

            processed.append((int(record.timestamp), row_values))

        column_names = measurement_order
        data_types = [data_types_map[name] for name in column_names]
        timestamps = [timestamp for timestamp, _ in processed]
        values: list[list[Any]] = [[row_values.get(name) for name in column_names] for _, row_values in processed]
        return column_names, data_types, timestamps, values

    def _resolve_table_name(self, normalized_name: str) -> str:
        table_name = normalized_name.strip()
        if not table_name:
            raise ValueError("Table name cannot be empty after normalization")

        prefix = self._settings.table_name_prefix
        if prefix and not table_name.startswith(prefix):
            table_name = f"{prefix}{table_name}"
        return table_name


class IoTDBService:
    """High-level helper for inserting records into IoTDB."""

    def __init__(self, settings_obj: IoTDBSettings, session_manager: IoTDBSessionManager):
        self._settings = settings_obj
        self._sessions = session_manager
        if settings_obj.sql_dialect == "table":
            self._writer: _BaseWriter = _TableIoTDBWriter(session_manager, settings_obj)
        else:
            self._writer = _TreeIoTDBWriter(session_manager, settings_obj)

    def write_records(self, target: str, records: Sequence[TimeSeriesRecord]) -> None:
        if not records:
            raise ValueError("records must contain at least one element")
        self._writer.write_records(target, records)


@lru_cache(maxsize=1)
def get_iotdb_service() -> IoTDBService:
    settings_obj = IoTDBSettings.from_django()
    manager = IoTDBSessionManager(settings_obj)
    return IoTDBService(settings_obj, manager)


def reset_iotdb_service_cache() -> None:
    try:
        cached_service = get_iotdb_service()
        # Attempt to close the session pool if present
        if hasattr(cached_service, "manager") and hasattr(cached_service.manager, "close"):
            cached_service.manager.close()
    except Exception as e:
        # Log any errors during close, but proceed to clear cache
        import logging

        logging.warning(f"Error closing IoTDBService session pool: {e}")
    get_iotdb_service.cache_clear()


__all__ = [
    "IoTDBService",
    "IoTDBSessionManager",
    "IoTDBSettings",
    "TimeSeriesRecord",
    "get_iotdb_service",
    "reset_iotdb_service_cache",
]
