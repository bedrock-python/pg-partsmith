"""
Tells whether the AI review of a branch covers its last commit, from the record the review writes to
local/reviews/<branch>.md (see README.md next to this file).

    check.py                say whether the review is missing or behind the branch; always exit 0
    check.py --strict       the same, but exit 1 unless the branch is reviewed up to its last commit
    check.py --refresh      when a rebase left the reviewed changes as they were under new commits,
                            move the record's commit to them

As the pre-push hook it checks the branch being pushed and never blocks the push: unfinished branches
are pushed to run CI. By hand it checks the branch checked out.

Only the standard library: it runs from pre-commit in a repository of any language.
"""

from __future__ import annotations

import argparse
import codecs
import hashlib
import os
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

RECORDS = Path("local", "reviews")
COMMIT = re.compile(r"^commit:[ \t]*([0-9a-f]{7,64})[ \t]*(?=\r?$)", re.MULTILINE)
# agents write the record in UTF-8; PowerShell 5.1 redirection writes UTF-16
BOMS = ((codecs.BOM_UTF16_LE, "utf-16"), (codecs.BOM_UTF16_BE, "utf-16"), (codecs.BOM_UTF8, "utf-8-sig"))
# the diffs are read by this script, not a person: no colour, external diff or textconv a config may turn on
PLAIN_DIFF = ("diff", "--no-color", "--no-ext-diff", "--no-textconv")
HUNK_LINES = re.compile(rb"^@@ -\d+(?:,\d+)? \+\d+(?:,\d+)? @@")
HOW = (
    "run it in your agent (/review-change, $review-change; .agents/review/README.md)"
    " before opening or updating the pull request"
)

REVIEWED, BEHIND, SKIPPED = "reviewed", "behind", "skipped"


@dataclass(frozen=True)
class Branch:
    name: str
    head: str
    base: str | None
    top: Path


@dataclass(frozen=True)
class Record:
    path: Path
    text: str
    codec: str | None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--strict", action="store_true", help="exit 1 unless the branch is reviewed up to its last commit"
    )
    parser.add_argument(
        "--refresh", action="store_true", help="move the record's commit to the same changes under new commits"
    )
    args = parser.parse_args()

    try:
        status, message = check(refresh=args.refresh)
    except Exception as error:  # noqa: BLE001, RUF100 - the hook must never block a push, so a broken check only says so
        status, message = BEHIND, f"AI review: the check failed: {error!r}"
    if status == SKIPPED and not args.strict:
        return 0
    sys.stdout.write(message + "\n")
    return 1 if args.strict and status != REVIEWED else 0


def check(*, refresh: bool) -> tuple[str, str]:
    branch, skipped = branch_to_check()
    if branch is None:
        return SKIPPED, skipped
    relative = RECORDS / f"{branch.name}.md"
    path = branch.top / relative
    if not path.is_file():
        return BEHIND, f"AI review of {branch.name}: not run yet; {HOW}"
    record = Record(path, *read_record(path))
    reviewed, missing = reviewed_commit(record.text, relative)
    if reviewed is None:
        return BEHIND, f"AI review of {branch.name}: {missing}; {HOW}"
    if reviewed == branch.head:
        return REVIEWED, f"AI review: {branch.name} is reviewed up to its last commit"
    if git("merge-base", "--is-ancestor", reviewed, branch.head) is not None:
        return since_review(branch, reviewed)
    return after_rewrite(branch, reviewed, record, refresh=refresh)


def branch_to_check() -> tuple[Branch | None, str]:
    """The branch to check, or none and why it is skipped."""
    top = git("rev-parse", "--show-toplevel")
    if top is None:
        return None, "AI review: not in a git work tree, or git is missing"
    name, head = pushed_branch()
    if name is None or head is None:
        return None, "AI review: no branch to check (detached HEAD)"
    base = base_ref()
    if name in default_names(base):
        return None, f"AI review: {name} is the default branch"
    return Branch(name, head, base, Path(top)), ""


def reviewed_commit(text: str, relative: Path) -> tuple[str | None, str]:
    """The commit the record names, or none and what is wrong with the record."""
    found = COMMIT.search(text)
    if found is None:
        return None, f"{relative.as_posix()} names no commit"
    reviewed = git("rev-parse", "--verify", "--quiet", f"{found.group(1)}^{{commit}}")
    if reviewed is None:
        return None, f"the reviewed commit {found.group(1)[:8]} is not in this clone"
    return reviewed, ""


def since_review(branch: Branch, reviewed: str) -> tuple[str, str]:
    changed = branch_files_changed(reviewed, branch.head, branch.base)
    if not changed:
        return REVIEWED, f"AI review: {branch.name} is reviewed; nothing of its own changed since {reviewed[:8]}"
    return (
        BEHIND,
        f"AI review of {branch.name}: {len(changed)} file(s) changed since the review at {reviewed[:8]}; {HOW}",
    )


def after_rewrite(branch: Branch, reviewed: str, record: Record, *, refresh: bool) -> tuple[str, str]:
    # a rebase keeps the reviewed changes under new commits: compare the changes, not the commits
    if branch.base is None or not same_changes(reviewed, branch.head, branch.base):
        return (
            BEHIND,
            f"AI review of {branch.name}: rewritten since the review at {reviewed[:8]}, with other changes; {HOW}",
        )
    if refresh and record.codec is not None:
        record.path.write_bytes(COMMIT.sub(f"commit: {branch.head}", record.text, count=1).encode(record.codec))
        return (
            REVIEWED,
            f"AI review: {branch.name} carries the reviewed changes; the record now names {branch.head[:8]}",
        )
    return REVIEWED, f"AI review: {branch.name} carries the changes reviewed at {reviewed[:8]} under new commits"


def pushed_branch() -> tuple[str | None, str | None]:
    local = os.environ.get("PRE_COMMIT_LOCAL_BRANCH")
    if local:
        # pre-commit's pre-push: the branch being pushed and its sha, which need not be the checked-out ones
        head = os.environ.get("PRE_COMMIT_TO_REF") or git("rev-parse", "--verify", "--quiet", local)
        if not local.startswith("refs/heads/"):
            # `git push origin HEAD` names no branch: it is the checked-out one when the shas agree
            if head is None or head != git("rev-parse", "--verify", "--quiet", "HEAD"):
                return None, None
            local = git("symbolic-ref", "--quiet", "HEAD")
            if not local:
                return None, None
        return local.removeprefix("refs/heads/"), head
    ref = git("symbolic-ref", "--quiet", "HEAD")
    if not ref:
        return None, None
    return ref.removeprefix("refs/heads/"), git("rev-parse", "--verify", "--quiet", ref)


def base_ref() -> str | None:
    named = git("symbolic-ref", "--quiet", "--short", "refs/remotes/origin/HEAD")
    candidates = [named] if named else ["origin/master", "origin/main"]
    return next((ref for ref in candidates if git("rev-parse", "--verify", "--quiet", ref)), None)


def default_names(base: str | None) -> set[str]:
    names = {"master", "main"}
    if base:
        names.add(base.split("/", 1)[-1])
    return names


def read_record(path: Path) -> tuple[str, str | None]:
    """The record's text and the codec to write it back with; no codec when it is not UTF-8 or UTF-16."""
    raw = path.read_bytes()
    for bom, codec in BOMS:
        if raw.startswith(bom):
            return raw.decode(codec, errors="replace"), codec
    try:
        return raw.decode("utf-8"), "utf-8"
    except UnicodeDecodeError:
        return raw.decode("utf-8", errors="replace"), None


def same_changes(reviewed: str, head: str, base: str) -> bool:
    before = changes_id(reviewed, base)
    return before is not None and before == changes_id(head, base)


def changes_id(commit: str, base: str) -> str | None:
    fork = git("merge-base", base, commit)
    if fork is None:
        return None
    diff = run(*PLAIN_DIFF, fork, commit)
    if not diff:
        return None
    # on a new base the same changes differ only in blob ids and line numbers; whitespace and context
    # lines still count, so a re-indented or moved line is a change to review again
    lines = [HUNK_LINES.sub(b"@@", line) for line in diff.split(b"\n") if not line.startswith(b"index ")]
    return hashlib.sha256(b"\n".join(lines)).hexdigest()


def branch_files_changed(reviewed: str, head: str, base: str | None) -> set[str]:
    changed = set((git(*PLAIN_DIFF, "--name-only", reviewed, head) or "").splitlines())
    if base is None:
        return changed
    # a merge of the default branch brings other people's changes: only the branch's own files count
    own = set((git(*PLAIN_DIFF, "--name-only", f"{base}...{head}") or "").splitlines())
    return changed & own


def git(*args: str) -> str | None:
    output = run(*args)
    return None if output is None else output.decode("utf-8", errors="replace").strip()


def run(*args: str) -> bytes | None:
    executable = shutil.which("git")
    if not executable:
        return None
    result = subprocess.run([executable, *args], capture_output=True, check=False)  # noqa: S603, RUF100
    return result.stdout if result.returncode == 0 else None


if __name__ == "__main__":
    sys.exit(main())
