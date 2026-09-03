"""What the process returns, because a CronJob and a CI step read that and nothing else."""

from enum import IntEnum

__all__ = ["ExitCode"]


class ExitCode(IntEnum):
    """The exit status of one command.

    Distinguishable on purpose: "nothing to do", "there is drift", "a human has
    to look at this" and "it did not run" are four different pages, and a tool
    that returns 1 for all of them is alerted on by nobody.

    Attributes:
        OK: The command did what it was asked, and found nothing pending.
        FAILED: Something unexpected. The message is on stderr.
        DRIFT: ``plan --check`` found operations waiting to be applied. Not a
            failure -- it is what "maintenance has not run lately" looks like.
        FINDINGS: The planner reported something an operator has to act on,
            or the database refused a statement -- a missing grant, a
            constraint. Outranks :attr:`DRIFT`: drift is what a run fixes, a
            finding is not.
        CONFIG: The document does not parse, or does not match the database.
        CONNECTION: The database could not be reached, refused the
            credentials, or dropped the connection.
        LOCKED: Another maintainer holds the table's lock. Overlapping runs are
            ordinary, so this is its own code rather than a failure to page on.
        USAGE: The invocation itself was wrong -- a misspelled flag, no command.
            ``EX_USAGE`` from ``sysexits.h``, and deliberately not the ``2``
            argument parsers default to: that is what this tool means "drift"
            by, and a CronJob alerting on drift must not page over a typo.
        INTERRUPTED: Stopped by ``SIGINT`` -- Ctrl+C -- after cleaning up.
            ``128 + 2``, the shell's own spelling of it.
        TERMINATED: Stopped by ``SIGTERM`` -- what a pod is sent at its
            deadline -- after cleaning up. ``128 + 15``, likewise.
    """

    OK = 0
    FAILED = 1
    DRIFT = 2
    FINDINGS = 3
    CONFIG = 4
    CONNECTION = 5
    LOCKED = 6
    USAGE = 64
    INTERRUPTED = 130
    TERMINATED = 143
