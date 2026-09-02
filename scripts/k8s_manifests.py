"""Copy the Kubernetes manifests out of a guide, one file per fenced block, for kubeconform.

Usage: python scripts/k8s_manifests.py docs/guide/running.md manifests/

A fenced ``yaml`` block is a manifest when its first document carries an
``apiVersion`` and a ``kind``; the guide's fragments -- a container spec to
paste, a lone ``args`` line -- have neither and are left where they are. Blocks
are written verbatim, several documents and comments included, so a line
number kubeconform reports is a line in the guide's block.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import yaml

FENCE = re.compile(r"^```yaml\n(.*?)^```\n", re.MULTILINE | re.DOTALL)


def manifests(markdown: str) -> list[str]:
    """The fenced YAML blocks of ``markdown`` whose first document is a Kubernetes object."""
    found: list[str] = []
    for block in FENCE.findall(markdown):
        first = next(iter(yaml.safe_load_all(block)), None)
        if isinstance(first, dict) and "apiVersion" in first and "kind" in first:
            found.append(block)
    return found


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        sys.stderr.write(__doc__ or "")
        return 64
    guide, target = Path(argv[1]), Path(argv[2])
    target.mkdir(parents=True, exist_ok=True)
    blocks = manifests(guide.read_text(encoding="utf-8"))
    for index, block in enumerate(blocks, start=1):
        kinds = "-".join(str(doc["kind"]).lower() for doc in yaml.safe_load_all(block) if isinstance(doc, dict))
        (target / f"{index:02d}-{kinds}.yaml").write_text(block, encoding="utf-8")
    sys.stdout.write(f"{len(blocks)} manifest blocks from {guide} in {target}\n")
    return 0 if blocks else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
