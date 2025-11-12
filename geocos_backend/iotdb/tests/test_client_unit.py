from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from geocos_backend.iotdb.client import (
    IoTDBService,
    IoTDBSessionManager,
    IoTDBSettings,
    TimeSeriesRecord,
    get_iotdb_service,
    reset_iotdb_service_cache,
)
from geocos_backend.iotdb.exceptions import IoTDBConfigurationError, IoTDBWriteError


class TestIoTDBSettings:
    def test_from_django_missing_config(self, settings):
        settings.IOTDB = None
        with pytest.raises(IoTDBConfigurationError, match="IOTDB settings are missing"):
            IoTDBSettings.from_django()

    def test_from_django_with_config(self, settings):
        settings.IOTDB = {
            "HOST": "localhost",
            "PORT": 6667,
            "USERNAME": "user",
            "PASSWORD": "pass",
            "FETCH_SIZE": 1000,
            "ZONE_ID": "UTC",
            "MAX_RETRY": 5,
            "POOL_SIZE": 10,
            "POOL_WAIT_TIMEOUT_MS": 5000,
            "USE_SSL": True,
            "CA_CERTS": "/path/to/certs",
            "NODE_URLS": ["node1:6667", "node2:6667"],
            "ENABLE_REDIRECTION": False,
        }
        settings_obj = IoTDBSettings.from_django()
        assert settings_obj.host == "localhost"
        assert settings_obj.port == 6667
        assert settings_obj.username == "user"
        assert settings_obj.password == "pass"
        assert settings_obj.fetch_size == 1000
        assert settings_obj.zone_id == "UTC"
        assert settings_obj.max_retry == 5
        assert settings_obj.pool_size == 10
        assert settings_obj.pool_wait_timeout_ms == 5000
        assert settings_obj.use_ssl is True
        assert settings_obj.ca_certs == "/path/to/certs"
        assert settings_obj.node_urls == ("node1:6667", "node2:6667")
        assert settings_obj.enable_redirection is False

    def test_from_django_defaults(self, settings):
        settings.IOTDB = {}
        settings_obj = IoTDBSettings.from_django()
        assert settings_obj.host == "127.0.0.1"
        assert settings_obj.port == 6667
        assert settings_obj.username == "root"
        assert settings_obj.password == "root"
        assert settings_obj.fetch_size == 1024
        assert settings_obj.zone_id == "UTC+8"
        assert settings_obj.max_retry == 3
        assert settings_obj.pool_size == 5
        assert settings_obj.pool_wait_timeout_ms == 3000
        assert settings_obj.use_ssl is False
        assert settings_obj.ca_certs is None
        assert settings_obj.node_urls == ()
        assert settings_obj.enable_redirection is True


class TestIoTDBSessionManager:
    def test_create_pool_invalid_pool_size(self):
        settings = IoTDBSettings(
            host="localhost",
            port=6667,
            username="root",
            password="root",
            fetch_size=1024,
            zone_id="UTC",
            max_retry=3,
            pool_size=0,
            pool_wait_timeout_ms=3000,
            use_ssl=False,
            ca_certs=None,
            node_urls=(),
            enable_redirection=True,
        )
        manager = IoTDBSessionManager(settings)
        with pytest.raises(ValueError, match="pool_size must be positive"):
            manager._create_pool()

    def test_create_pool_invalid_timeout(self):
        settings = IoTDBSettings(
            host="localhost",
            port=6667,
            username="root",
            password="root",
            fetch_size=1024,
            zone_id="UTC",
            max_retry=3,
            pool_size=5,
            pool_wait_timeout_ms=0,
            use_ssl=False,
            ca_certs=None,
            node_urls=(),
            enable_redirection=True,
        )
        manager = IoTDBSessionManager(settings)
        with pytest.raises(ValueError, match="pool_wait_timeout_ms must be positive"):
            manager._create_pool()

    @patch("geocos_backend.iotdb.client.SessionPool")
    def test_create_pool_with_node_urls(self, mock_pool_class):
        settings = IoTDBSettings(
            host="localhost",
            port=6667,
            username="root",
            password="root",
            fetch_size=1024,
            zone_id="UTC",
            max_retry=3,
            pool_size=5,
            pool_wait_timeout_ms=3000,
            use_ssl=False,
            ca_certs=None,
            node_urls=("node1:6667", "node2:6667"),
            enable_redirection=True,
        )
        manager = IoTDBSessionManager(settings)
        manager._create_pool()

    @patch("geocos_backend.iotdb.client.SessionPool")
    def test_create_pool_with_ssl(self, mock_pool_class):
        settings = IoTDBSettings(
            host="localhost",
            port=6667,
            username="root",
            password="root",
            fetch_size=1024,
            zone_id="UTC",
            max_retry=3,
            pool_size=5,
            pool_wait_timeout_ms=3000,
            use_ssl=True,
            ca_certs="/certs",
            node_urls=(),
            enable_redirection=True,
        )
        manager = IoTDBSessionManager(settings)
        manager._create_pool()

    @patch("geocos_backend.iotdb.client.SessionPool")
    def test_acquire_and_put_back(self, mock_pool_class):
        mock_pool = MagicMock()
        mock_session = MagicMock()
        mock_pool.get_session.return_value = mock_session
        mock_pool_class.return_value = mock_pool

        settings = IoTDBSettings(
            host="localhost",
            port=6667,
            username="root",
            password="root",
            fetch_size=1024,
            zone_id="UTC",
            max_retry=3,
            pool_size=5,
            pool_wait_timeout_ms=3000,
            use_ssl=False,
            ca_certs=None,
            node_urls=(),
            enable_redirection=True,
        )
        manager = IoTDBSessionManager(settings)
        with manager.acquire() as session:
            assert session == mock_session
        mock_pool.get_session.assert_called_once()
        mock_pool.put_back.assert_called_once_with(mock_session)

    @patch("geocos_backend.iotdb.client.SessionPool")
    def test_close(self, mock_pool_class):
        mock_pool = MagicMock()
        mock_pool_class.return_value = mock_pool

        settings = IoTDBSettings(
            host="localhost",
            port=6667,
            username="root",
            password="root",
            fetch_size=1024,
            zone_id="UTC",
            max_retry=3,
            pool_size=5,
            pool_wait_timeout_ms=3000,
            use_ssl=False,
            ca_certs=None,
            node_urls=(),
            enable_redirection=True,
        )
        manager = IoTDBSessionManager(settings)
        manager._ensure_pool()
        manager.close()
        mock_pool.close.assert_called_once()
        assert manager._pool is None


class TestIoTDBService:
    def test_write_records_empty_records(self):
        manager = MagicMock()
        service = IoTDBService(manager)
        with pytest.raises(ValueError, match="records must contain at least one element"):
            service.write_records("device", [])

    def test_write_records_empty_measurements(self):
        manager = MagicMock()
        service = IoTDBService(manager)
        records = [TimeSeriesRecord(timestamp=1, measurements={})]
        with pytest.raises(ValueError, match="Each record must provide at least one measurement"):
            service.write_records("device", records)

    def test_write_records_invalid_measurement_name(self):
        manager = MagicMock()
        service = IoTDBService(manager)
        records = [TimeSeriesRecord(timestamp=1, measurements={"": 1})]
        with pytest.raises(ValueError, match="Measurement names must be non-empty strings"):
            service.write_records("device", records)

    def test_write_records_invalid_measurement_name_type(self):
        manager = MagicMock()
        service = IoTDBService(manager)
        records = [TimeSeriesRecord(timestamp=1, measurements={123: 1})]  # type: ignore
        with pytest.raises(ValueError, match="Measurement names must be non-empty strings"):
            service.write_records("device", records)

    @pytest.mark.parametrize(
        ("value", "expected_type"),
        [
            (True, "BOOLEAN"),
            (42, "INT64"),
            (3.14, "DOUBLE"),
            ("text", "TEXT"),
        ],
    )
    def test_coerce_value_supported_types(self, value, expected_type):
        from iotdb.utils.IoTDBConstants import TSDataType

        coerced, data_type = IoTDBService._coerce_value(value)
        assert coerced == value
        assert data_type == getattr(TSDataType, expected_type)

    def test_coerce_value_decimal(self):
        from decimal import Decimal

        from iotdb.utils.IoTDBConstants import TSDataType

        value = Decimal("3.14")
        coerced, data_type = IoTDBService._coerce_value(value)
        assert coerced == pytest.approx(3.14)
        assert data_type == TSDataType.DOUBLE

    def test_coerce_value_unsupported_type(self):
        with pytest.raises(TypeError, match="Unsupported measurement value type"):
            IoTDBService._coerce_value([1, 2, 3])

    @patch("geocos_backend.iotdb.client.SessionPool")
    def test_write_records_success(self, mock_pool_class):
        mock_pool = MagicMock()
        mock_session = MagicMock()
        mock_pool.get_session.return_value = mock_session
        mock_pool_class.return_value = mock_pool

        manager = IoTDBSessionManager(
            IoTDBSettings(
                host="localhost",
                port=6667,
                username="root",
                password="root",
                fetch_size=1024,
                zone_id="UTC",
                max_retry=3,
                pool_size=5,
                pool_wait_timeout_ms=3000,
                use_ssl=False,
                ca_certs=None,
                node_urls=(),
                enable_redirection=True,
            )
        )
        service = IoTDBService(manager)
        records = [
            TimeSeriesRecord(timestamp=1000, measurements={"temp": 20.5, "status": True}),
            TimeSeriesRecord(timestamp=2000, measurements={"temp": 21.0}),
        ]
        service.write_records("device1", records)
        mock_session.insert_records_of_one_device.assert_called_once()
        args = mock_session.insert_records_of_one_device.call_args[0]
        assert args[0] == "device1"
        assert args[1] == [1000, 2000]
        assert args[2] == [["temp", "status"], ["temp"]]
        # Check data types and values

    @patch("geocos_backend.iotdb.client.SessionPool")
    def test_write_records_iotdb_error(self, mock_pool_class):
        mock_pool = MagicMock()
        mock_session = MagicMock()
        mock_session.insert_records_of_one_device.side_effect = Exception("IoTDB error")
        mock_pool.get_session.return_value = mock_session
        mock_pool_class.return_value = mock_pool

        manager = IoTDBSessionManager(
            IoTDBSettings(
                host="localhost",
                port=6667,
                username="root",
                password="root",
                fetch_size=1024,
                zone_id="UTC",
                max_retry=3,
                pool_size=5,
                pool_wait_timeout_ms=3000,
                use_ssl=False,
                ca_certs=None,
                node_urls=(),
                enable_redirection=True,
            )
        )
        service = IoTDBService(manager)
        records = [TimeSeriesRecord(timestamp=1000, measurements={"temp": 20.5})]
        with pytest.raises(IoTDBWriteError, match="Failed to write records to IoTDB"):
            service.write_records("device1", records)


class TestGlobalFunctions:
    @patch("geocos_backend.iotdb.client.IoTDBSettings.from_django")
    @patch("geocos_backend.iotdb.client.IoTDBSessionManager")
    def test_get_iotdb_service_caching(self, mock_manager_class, mock_from_django):
        mock_settings = MagicMock()
        mock_from_django.return_value = mock_settings
        mock_manager = MagicMock()
        mock_manager_class.return_value = mock_manager
        mock_service = MagicMock()
        with patch("geocos_backend.iotdb.client.IoTDBService", return_value=mock_service):
            service1 = get_iotdb_service()
            service2 = get_iotdb_service()
            assert service1 is service2
            assert service1 is mock_service
            mock_from_django.assert_called_once()
            mock_manager_class.assert_called_once_with(mock_settings)

    def test_reset_iotdb_service_cache(self):
        # Clear any existing cache
        reset_iotdb_service_cache()
        # Since it's lru_cache, we can check by calling and seeing if from_django is called again
        with patch("geocos_backend.iotdb.client.IoTDBSettings.from_django") as mock_from_django:
            mock_from_django.return_value = MagicMock()
            with patch("geocos_backend.iotdb.client.IoTDBSessionManager"):
                with patch("geocos_backend.iotdb.client.IoTDBService"):
                    get_iotdb_service()
                    assert mock_from_django.call_count == 1
                    reset_iotdb_service_cache()
                    get_iotdb_service()
                    assert mock_from_django.call_count == 2
