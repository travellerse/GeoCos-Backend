from __future__ import annotations

import os
import time
import uuid
from typing import Iterator, cast

import pytest
from docker.errors import DockerException
from iotdb.Session import Session
from requests import exceptions as requests_exceptions
from testcontainers.core.container import DockerContainer

from cosray_backend.iotdb.client import (
    TimeSeriesRecord,
    get_iotdb_service,
    reset_iotdb_service_cache,
)

IOTDB_VERSION = os.getenv("IOTDB_TEST_VERSION", "2.0.5-standalone")
os.environ.setdefault("TESTCONTAINERS_RYUK_DISABLED", "true")


def _wait_for_iotdb(host: str, port: int, *, timeout: float = 120.0, interval: float = 2.0) -> None:
    """Poll IoTDB until it accepts connections or the timeout elapses."""

    deadline = time.time() + timeout
    last_error: Exception | None = None

    while time.time() < deadline:
        session = Session(host, port, "root", "root")
        try:
            session.open(False)
            session.close()
            return
        except Exception as exc:  # pragma: no cover - transient startup errors
            last_error = exc
            time.sleep(interval)

    raise RuntimeError("IoTDB container did not become ready") from last_error


@pytest.fixture(scope="session")
def iotdb_docker() -> Iterator[tuple[str, int]]:
    image = os.getenv("IOTDB_TEST_IMAGE", f"apache/iotdb:{IOTDB_VERSION}")
    try:
        container = DockerContainer(image).with_exposed_ports(6667)
    except (DockerException, requests_exceptions.ConnectionError) as exc:  # pragma: no cover - environment specific
        pytest.skip(f"Docker not available: {exc}")

    try:
        with container as running:
            host = running.get_container_host_ip()
            port = int(running.get_exposed_port(6667))
            _wait_for_iotdb(host, port)
            yield host, port
    except (DockerException, requests_exceptions.ConnectionError) as exc:  # pragma: no cover - environment specific
        pytest.skip(f"Docker not available: {exc}")


@pytest.fixture()
def configured_iotdb(settings, iotdb_docker: tuple[str, int]) -> Iterator[dict[str, object]]:
    host, port = iotdb_docker

    config = dict(settings.IOTDB) | {
        "HOST": host,
        "PORT": port,
        "USERNAME": "root",
        "PASSWORD": "root",
        "NODE_URLS": (),
        "USE_SSL": False,
    }
    settings.IOTDB = config

    reset_iotdb_service_cache()
    try:
        yield config
    finally:
        reset_iotdb_service_cache()


@pytest.mark.django_db
def test_write_records_against_iotdb_container(configured_iotdb: dict[str, object]) -> None:
    host = cast(str, configured_iotdb["HOST"])
    port = int(cast(int | str, configured_iotdb["PORT"]))
    username = cast(str, configured_iotdb["USERNAME"])
    password = cast(str, configured_iotdb["PASSWORD"])
    storage_group = cast(str, configured_iotdb.get("ROOT_PATH", "root.cosray"))

    session = Session(host, port, username, password)
    session.open(False)
    try:
        session.set_storage_group(storage_group)
    except Exception:
        # Storage group already exists; safe to ignore.
        pass
    finally:
        session.close()
    device_suffix = uuid.uuid4().hex
    device_path = f"{storage_group}.pytest_{device_suffix}"

    base_timestamp = int(time.time() * 1000)
    records = [
        TimeSeriesRecord(timestamp=base_timestamp + offset, measurements={"temperature": 23.5 + offset})
        for offset in range(2)
    ]

    service = get_iotdb_service()
    service.write_records(device_path, records)

    session = Session(host, port, username, password)
    session.open(False)
    try:
        result = session.execute_query_statement(f"select temperature from {device_path}")
        df = result.todf()
    finally:
        session.close()

    assert not df.empty
    temperature_column = f"{device_path}.temperature"
    assert temperature_column in df.columns
    assert df[temperature_column].tolist() == pytest.approx([23.5, 24.5])
