"""PostgreSQL partitioning constants and default values."""

# Default configuration values.
DEFAULT_CREATE_AHEAD_COUNT = 6
DEFAULT_RETENTION_COUNT = 12

# PostgreSQL limits.
MAX_IDENTIFIER_LENGTH = 63

# Default timeouts and intervals.
DEFAULT_LOCK_TIMEOUT_SECONDS = 10
DEFAULT_MAINTENANCE_TIMEOUT_SECONDS = 300

# Calendar bounds for Period validation.
MIN_MONTH = 1
MAX_MONTH = 12
MIN_ISO_WEEK = 1
MAX_ISO_WEEK = 53
