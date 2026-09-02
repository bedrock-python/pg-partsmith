#!/usr/bin/env bash
# An after_create / before_detach hook: post one line to a webhook.
# Exits non-zero only if the webhook is unreachable -- which, for an
# after_* phase, abandons what is left of the operation and lets the next
# run plan it again. Drop the `-f` if a failed notification should never
# stop maintenance.
set -euo pipefail

cat >/dev/null                     # the event on stdin; this one only needs the environment
url="${WEBHOOK_URL:?set WEBHOOK_URL}"
text="pg-partsmith: ${PG_PARTSMITH_PHASE} ${PG_PARTSMITH_PARTITION} (${PG_PARTSMITH_TABLE})"

curl -fsS -X POST -H 'Content-Type: application/json' \
  --data "{\"text\": \"${text}\"}" "$url" >/dev/null
