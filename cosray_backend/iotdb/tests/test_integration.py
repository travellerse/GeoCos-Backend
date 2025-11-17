from __future__ import annotations

import os
import time
from typing import Any, Iterator, Mapping, cast

os.environ.setdefault("TESTCONTAINERS_RYUK_DISABLED", "true")

import pytest
from iotdb.Session import Session

pytest.importorskip("testcontainers")

from docker.errors import DockerException  # type: ignore[import]
from testcontainers.core.container import DockerContainer  # noqa: E402
from testcontainers.core.wait_strategies import PortWaitStrategy  # noqa: E402

from cosray_backend.iotdb.client import (  # noqa: E402
    IoTDBService,
    IoTDBSessionManager,
    IoTDBSettings,
    TimeSeriesRecord,
)

_DEFAULT_IMAGE = os.environ.get("TEST_IOTDB_IMAGE", "apache/iotdb:2.0.5-standalone")
_IOTDB_PORT = 6667
_USERNAME = os.environ.get("TEST_IOTDB_USERNAME", "root")
_PASSWORD = os.environ.get("TEST_IOTDB_PASSWORD", "root")
_STORAGE_GROUP = "root.integration"


class IoTDBTestContainer(DockerContainer):
    def __init__(self, image: str = _DEFAULT_IMAGE) -> None:
        super().__init__(image)
        self._port = _IOTDB_PORT
        self.with_exposed_ports(self._port)

        self.waiting_for(PortWaitStrategy(self._port).with_startup_timeout(60))


@pytest.fixture(scope="module")
def iotdb_endpoint() -> Iterator[tuple[str, int]]:
    # 环境变量指定了 IoTDB 实例的主机和端口，则直接使用该实例进行测试
    if env_host := os.environ.get("IOTDB_HOST"):
        host = env_host
        port = int(os.environ.get("IOTDB_PORT", _IOTDB_PORT))
        yield host, port
    else:  # 否则启动一个临时的 IoTDB 容器进行测试
        try:
            with IoTDBTestContainer() as container:
                host = container.get_container_host_ip()
                port = int(container.get_exposed_port(_IOTDB_PORT))
                yield host, port
        except (ConnectionError, DockerException) as exc:
            pytest.skip(f"IoTDB integration tests require Docker access: {exc}")


def _build_settings(host: str, port: int) -> IoTDBSettings:
    return IoTDBSettings(
        host=host,
        port=port,
        username=_USERNAME,
        password=_PASSWORD,
        fetch_size=1024,
        zone_id="UTC",
        max_retry=1,
        pool_size=1,
        pool_wait_timeout_ms=5000,
        use_ssl=False,
        ca_certs=None,
        node_urls=(),
        enable_redirection=False,
        enable_compression=False,
        connection_timeout_ms=5000,
        sql_dialect="tree",
        database=None,
        table_name_prefix=None,
    )


def _ensure_storage_group(host: str, port: int, storage_group: str) -> None:
    session = Session(host, port, _USERNAME, _PASSWORD)
    try:
        session.open(False)
        try:
            session.set_storage_group(storage_group)
        except Exception:  # pragma: no cover - already created storage group
            pass
    finally:
        session.close()


def _fetch_rows(host: str, port: int, sql: str) -> list[tuple[int, list[str]]]:
    session = Session(host, port, _USERNAME, _PASSWORD)
    try:
        session.open(False)
        dataset = session.execute_query_statement(sql)
        rows: list[tuple[int, list[str]]] = []
        while dataset.has_next():
            next_row = dataset.next()
            if next_row is None:  # pragma: no cover - defensive guard
                continue
            timestamp = next_row.get_timestamp()
            fields = [field.get_string_value() for field in next_row.get_fields()]
            rows.append((timestamp, fields))
        dataset.close_operation_handle()
        return rows
    finally:
        session.close()


def test_iotdb_service_can_insert_records_in_tree_dialect(iotdb_endpoint: tuple[str, int]) -> None:
    host, port = iotdb_endpoint
    _ensure_storage_group(host, port, _STORAGE_GROUP)

    settings = _build_settings(host, port)
    service = IoTDBService(settings, IoTDBSessionManager(settings))

    device = f"{_STORAGE_GROUP}.device1"
    records = [
        TimeSeriesRecord(timestamp=1_000, measurements={"temperature": 21.5, "status": "OK"}),
        TimeSeriesRecord(timestamp=2_000, measurements={"temperature": 22.0, "status": "WARN"}),
    ]

    service.write_records(device, records)

    result = _fetch_rows(host, port, f"SELECT temperature, status FROM {device}")

    assert [timestamp for timestamp, _ in result] == [1_000, 2_000]
    temperatures = [float(values[0]) for _, values in result]
    statuses = [values[1] for _, values in result]
    assert temperatures == [21.5, 22.0]
    assert statuses == ["OK", "WARN"]


def test_iotdb_service_write_records_with_invalid_data_raises(iotdb_endpoint: tuple[str, int]) -> None:
    host, port = iotdb_endpoint
    _ensure_storage_group(host, port, _STORAGE_GROUP)

    settings = _build_settings(host, port)
    service = IoTDBService(settings, IoTDBSessionManager(settings))

    device = f"{_STORAGE_GROUP}.device1"
    # Invalid: measurements should be a dict of str->(float/int/str), but here we use a list
    invalid_records = [
        TimeSeriesRecord(
            timestamp=3_000,
            measurements=cast(Mapping[str, Any], ["not", "a", "dict"]),
        ),
    ]

    with pytest.raises(Exception):  # Replace Exception with the specific error if known
        service.write_records(device, invalid_records)
