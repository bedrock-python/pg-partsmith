"""PostgreSQL partitioning constants and default values."""

# Default configuration values.
DEFAULT_CREATE_AHEAD_COUNT = 6
DEFAULT_RETENTION_COUNT = 12

# PostgreSQL limits.
MAX_IDENTIFIER_LENGTH = 63

# Subpartitioning defaults and guard rails.
DEFAULT_HASH_NAME_SUFFIX = "__h{remainder}"
DEFAULT_LIST_NAME_SUFFIX = "__{name}"
DEFAULT_LIST_DEFAULT_NAME = "other"
DEFAULT_NUMERIC_NAME_SUFFIX = "__{start}"
DEFAULT_SEQUENCE_NAME_SUFFIX = "__{value}"
# Depth of a declared partition scheme, root level included.
MAX_SCHEME_DEPTH = 5
# Largest least-common-multiple of hash moduli we will enumerate when checking
# whether a mixed-modulus hash set tiles the keyspace; beyond it coverage is
# reported as unknown rather than guessed.
MAX_HASH_KEYSPACE_LCM = 1 << 16

# Repository defaults.
DEFAULT_DDL_TIMEOUT_SECONDS = 30.0
DEFAULT_DDL_TIMEZONE = "UTC"
DEFAULT_DROP_LOCK_TIMEOUT_MS = 3000
DEFAULT_DROP_MAX_RETRIES = 3
DEFAULT_DROP_RETRY_DELAY = 0.5
DEFAULT_DROP_MAX_BACKOFF = 300.0

# Hashed into advisory lock IDs / Redis keys; changing it breaks cross-version
# mutual exclusion between deployments.
DEFAULT_LOCK_PREFIX = "partitioner"

# PostgreSQL SQLSTATEs.
PG_CHECK_VIOLATION = "23514"
# States that indicate the partition is already attached or duplicated (a race
# with another worker): duplicate_table, duplicate_object, and wrong_object_type
# (42809 is what PostgreSQL raises for "X is already a partition").
ATTACH_CONFLICT_SQLSTATES = frozenset({"42P07", "42710", "42809"})

# Retries for ATTACH after DEFAULT-partition reconciliation.
DEFAULT_CONFLICT_MAX_RETRIES = 2

# Calendar bounds for Period validation.
MIN_MONTH = 1
MAX_MONTH = 12
MIN_ISO_WEEK = 1
MAX_ISO_WEEK = 53
MIN_HOUR = 0
MAX_HOUR = 23
MIN_QUARTER = 1
MAX_QUARTER = 4
