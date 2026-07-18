class TLCPlatformError(Exception):
    """Base exception for the project."""


class ConfigurationError(TLCPlatformError, ValueError):
    """Raised when YAML configuration is invalid."""


class DownloadError(TLCPlatformError):
    """Raised when a remote file cannot be downloaded safely."""


class ParquetValidationError(TLCPlatformError):
    """Raised when a downloaded file is not an acceptable Bronze Parquet."""


class InsufficientDiskSpaceError(TLCPlatformError):
    """Raised when the local filesystem cannot safely receive the download."""

class SilverTransformationError(TLCPlatformError):
    """Raised when a Bronze partition cannot be transformed to Silver."""


class SilverReconciliationError(TLCPlatformError):
    """Raised when Bronze and Silver row counts cannot be reconciled."""

class SparkTemporarySpaceError(RuntimeError):
    """Raised when Spark temporary storage reaches the configured safety limit."""
