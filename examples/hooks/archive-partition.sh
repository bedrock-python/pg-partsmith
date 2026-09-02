#!/usr/bin/env bash
# A before_drop hook: dump the partition somewhere durable, and refuse the
# drop if that did not work. A non-zero exit is how a hook says "not yet".
#
# The whole event arrives as JSON on stdin. The facts a script usually needs
# are also in the environment, so jq is optional:
#   PG_PARTSMITH_PHASE       before_drop
#   PG_PARTSMITH_TABLE       public.events
#   PG_PARTSMITH_PARTITION   public.events__2025_08
#   PG_PARTSMITH_WINDOW_START / _END   ISO 8601, when the partition covers a period
#
# Needs: pg_dump on PATH, PGHOST/PGUSER/PGPASSWORD (or a .pgpass) for it,
# and ARCHIVE_DIR writable. Everything else is inherited from the run.
set -euo pipefail

event=$(cat)                       # keep it; jq can read the rest from here
partition="${PG_PARTSMITH_PARTITION:?not run as a pg-partsmith hook}"
archive_dir="${ARCHIVE_DIR:-/var/lib/pg-partsmith/archive}"
target="${archive_dir}/${partition//./_}.dump"

if command -v jq >/dev/null; then
  size=$(jq -r '.operation.size_bytes // "unknown"' <<<"$event")
  reason=$(jq -r '.operation.reason' <<<"$event")
  echo "archiving ${partition} (${size} bytes, ${reason}) to ${target}"
else
  echo "archiving ${partition} to ${target}"
fi

mkdir -p "$archive_dir"
# --table takes the schema-qualified name; -Fc is compressed and restorable
# with pg_restore. Write to a temp name and rename, so a half-written dump is
# never mistaken for a finished one.
pg_dump --format=custom --table="$partition" --file="${target}.part"
mv "${target}.part" "$target"
echo "archived ${partition}: $(wc -c <"$target") bytes"
