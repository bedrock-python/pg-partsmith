# A before_detach hook written as a block of Python, referenced from the
# document as `python_file: hooks/export_partition.py`.
#
# Two names are in scope: `event`, the PartitionEvent (the same object a
# hook class is handed), and `log`, a logger named for the phase. Anything
# else is imported, the way any Python is. Raising refuses the operation;
# the partition stays attached and the next run plans it again.
#
# This one copies the partition's rows out with COPY, through psql, into a
# directory an archiver picks up, and records what it did next to the file.
import json
import os
import shutil
import subprocess
from pathlib import Path

if shutil.which("psql") is None:
    msg = "psql is not on PATH; refusing to detach without an export"
    raise RuntimeError(msg)

target_dir = Path(os.environ.get("EXPORT_DIR", "/var/lib/pg-partsmith/export"))
target_dir.mkdir(parents=True, exist_ok=True)
stem = event.partition.name.replace(".", "_")
data = target_dir / f"{stem}.csv"
manifest = target_dir / f"{stem}.json"

log.info("exporting %s to %s", event.partition.name, data)
with data.open("wb") as out:
    subprocess.run(
        ["psql", "--no-psqlrc", "--command", f"COPY {event.partition.name} TO STDOUT WITH (FORMAT csv, HEADER)"],
        stdout=out,
        check=True,
    )

manifest.write_text(
    json.dumps(
        {
            "table": event.table_name,
            "partition": event.partition.name,
            "window": None if event.window is None else [event.window.start.isoformat(), event.window.end.isoformat()],
            "reason": event.operation.reason.value,
            "size_bytes": event.operation.size_bytes,
            "rows_estimate": event.operation.row_estimate,
        },
        indent=2,
    ),
    encoding="utf-8",
)
log.info("exported %s: %d bytes", event.partition.name, data.stat().st_size)
