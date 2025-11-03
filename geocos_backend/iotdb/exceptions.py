class IoTDBError(Exception):
    """Base exception for IoTDB integration."""


class IoTDBConfigurationError(IoTDBError):
    """Raised when IoTDB is not configured correctly."""


class IoTDBWriteError(IoTDBError):
    """Raised when writing data to IoTDB fails."""
