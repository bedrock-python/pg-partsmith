"""Write the Markdown source of every page next to the page it built.

The docs carry a "Copy page" control that hands a language model the Markdown
behind the page it is looking at. That Markdown has to be fetchable, so after
the site is built each ``docs/<path>.md`` is copied to ``site/<path>.md`` --
one URL away from ``site/<path>/index.html``, which is what the control asks
for. Run it after the build::

    uv run --no-dev --group docs zensical build --clean
    uv run python scripts/emit_markdown.py

``zensical serve`` rebuilds into the same directory and does not know about
these files, so a preview served that way answers 404 to the control; build
the site to try it.
"""

from __future__ import annotations

import pathlib
import shutil
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
DOCS = ROOT / "docs"
SITE = ROOT / "site"


def target_for(source: pathlib.Path) -> pathlib.Path:
    """Where the control will look for one page's Markdown.

    It asks for the page's own URL with ``.md`` in place of the trailing
    slash, so ``guide/foreign-keys/`` becomes ``guide/foreign-keys.md``. A
    section index is the one place where that is not the source path: the URL
    of ``reference/index.md`` is ``reference/``, and its Markdown therefore
    belongs at ``reference.md``. The site's own index keeps its name.
    """
    relative = source.relative_to(DOCS)
    if relative.name == "index.md" and relative.parent != pathlib.Path():
        return SITE / relative.parent.with_suffix(".md")
    return SITE / relative


def main() -> None:
    """Copy every documentation page into the built site beside its HTML."""
    if not SITE.is_dir():
        sys.exit(f"{SITE} does not exist -- build the site first")

    written: dict[pathlib.Path, pathlib.Path] = {}
    for source in sorted(DOCS.rglob("*.md")):
        target = target_for(source)
        if target in written:
            sys.exit(f"{source} and {written[target]} both claim {target}")
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
        written[target] = source

    sys.stdout.write(f"wrote {len(written)} Markdown pages into {SITE.name}/\n")


if __name__ == "__main__":
    main()
