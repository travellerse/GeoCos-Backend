from __future__ import annotations

from contextlib import contextmanager
from typing import cast

import pytest
from iotdb.utils.IoTDBConstants import TSDataType
from iotdb.utils.Tablet import ColumnType

from cosray_backend.iotdb.client import (
    IoTDBService,
    IoTDBSessionManager,
    IoTDBSettings,
    TimeSeriesRecord,
)


class DummySessionManager:
    def __init__(self, session):
        self._session = session
        self.acquire_calls = 0

    @contextmanager
    def acquire(self):
        self.acquire_calls += 1
        try:
            yield self._session
        finally:
            close = getattr(self._session, "close", None)
            if callable(close):
                close()

    def close(self) -> None:  # pragma: no cover - compatibility shim
        pass


class DummyTreeSession:
    def __init__(self):
        self.calls: list[dict[str, object]] = []

    def insert_records_of_one_device(self, device, time_list, measurements_list, data_types_list, values_list):
        self.calls.append(
            {
                "device": device,
                "time_list": time_list,
                "measurements": measurements_list,
                "data_types": data_types_list,
                "values": values_list,
            }
        )


class DummyTableSession:
    def __init__(self):
        self.tablets = []
        self.closed = False

    def insert(self, tablet):
        self.tablets.append(tablet)

    def close(self):
        self.closed = True


def _build_settings(sql_dialect: str, **overrides) -> IoTDBSettings:
    base_kwargs = {
        "host": "127.0.0.1",
        "port": 6667,
        "username": "root",
        "password": "root",
        "fetch_size": 1024,
        "zone_id": "UTC+8",
        "max_retry": 3,
        "pool_size": 2,
        "pool_wait_timeout_ms": 5000,
        "use_ssl": False,
        "ca_certs": None,
        "node_urls": (),
        "enable_redirection": True,
        "enable_compression": False,
        "connection_timeout_ms": None,
        "sql_dialect": sql_dialect,
        "database": None,
        "table_name_prefix": None,
    }
    base_kwargs.update(overrides)
    return IoTDBSettings(**base_kwargs)


def test_tree_service_writes_records_with_expected_payload():
    session = DummyTreeSession()
    manager = DummySessionManager(session)
    settings = _build_settings("tree")
    service = IoTDBService(settings, cast(IoTDBSessionManager, manager))

    records = [
        TimeSeriesRecord(timestamp=1, measurements={"m1": 10, "m2": 2.5}),
        TimeSeriesRecord(timestamp=2, measurements={"m1": 20, "m2": 5.0}),
    ]

    service.write_records("root.test.device", records)

    assert manager.acquire_calls == 1
    assert len(session.calls) == 1
    call = session.calls[0]
    assert call["device"] == "root.test.device"
    assert call["time_list"] == [1, 2]
    assert call["measurements"] == [["m1", "m2"], ["m1", "m2"]]
    assert call["data_types"] == [
        [TSDataType.INT64, TSDataType.DOUBLE],
        [TSDataType.INT64, TSDataType.DOUBLE],
    ]
    assert call["values"] == [[10, 2.5], [20, 5.0]]


def test_table_service_builds_tablet_with_expected_shape():
    session = DummyTableSession()
    manager = DummySessionManager(session)
    settings = _build_settings("table", table_name_prefix="cosray_")
    service = IoTDBService(settings, cast(IoTDBSessionManager, manager))

    records = [
        TimeSeriesRecord(timestamp=1_000, measurements={"m1": 42, "m2": 1.5}),
        TimeSeriesRecord(timestamp=2_000, measurements={"m2": 3.5, "m3": "status"}),
    ]

    service.write_records("detector", records)

    assert manager.acquire_calls == 1
    assert session.closed is True
    assert len(session.tablets) == 1

    tablet = session.tablets[0]
    assert tablet.get_insert_target_name() == "cosray_detector"
    assert tablet.get_measurements() == ["m1", "m2", "m3"]
    assert tablet.get_data_types() == [TSDataType.INT64, TSDataType.DOUBLE, TSDataType.TEXT]
    assert tablet.get_column_categories() == ColumnType.FIELD.n_copy(3)
    assert tablet.get_row_number() == 2

    # Access private storage for detailed value assertions.
    values = tablet._Tablet__values  # noqa: SLF001
    timestamps = tablet._Tablet__timestamps  # noqa: SLF001

    assert timestamps == [1_000, 2_000]
    assert values == [[42, 1.5, None], [None, 3.5, "status"]]


def test_table_writer_raises_on_inconsistent_types():
    session = DummyTableSession()
    manager = DummySessionManager(session)
    settings = _build_settings("table")
    service = IoTDBService(settings, cast(IoTDBSessionManager, manager))

    records = [
        TimeSeriesRecord(timestamp=1, measurements={"m1": 1}),
        TimeSeriesRecord(timestamp=2, measurements={"m1": "text"}),
    ]

    with pytest.raises(TypeError):
        service.write_records("table_name", records)
