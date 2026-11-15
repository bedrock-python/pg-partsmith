# Correctness

Does the changed code do what it means to, for every input and state it will meet? You are looking for bugs: results that are wrong, failures that are silent, state that is never cleaned up.

Read each changed function in full, its callers (search for them) and the code it calls. For every branch the change adds or alters, ask what input reaches it and what comes out.

## Look for

- **Logic**: an inverted or incomplete condition, an off-by-one, the wrong variable or field, a branch that can never run, a loop that stops too early or never, a default argument or a module-level value shared and mutated between calls.
- **Inputs**: `None`, empty, zero, negative, very large, duplicates, unicode, a missing key; time zones and naive datetimes, float rounding of money or counts, string and number coercion, an enum value the code does not handle.
- **Errors**: an exception swallowed or caught too broadly, a fallback or default that hides missing data, an error turned into success or into the wrong status, a lost cause (`raise ... from`), a retry of something that is not idempotent: "Fail loudly" in `AGENTS.md` and the guidelines on errors. A new failure nobody would notice in production: no error log where it fails, no metric or alert where the team already watches the neighbouring path. Never ask for a metric or alert by default; a temporary one for a launch carries a TODO with its issue and the condition to remove it.
- **Concurrency**: a check and an act that another request or task can come between, mutable state shared across requests, a missing `await`, a task started and never awaited, cancelled or kept, a lock held while awaiting a slow call, cleanup that does not run on cancellation or when the client disconnects. For every changed write path, run two of its requests at once in your head, and run it against every other path that writes the same rows or keys: a lost update (a counter or balance read and written back), a duplicate that ends as a server error instead of a conflict, two transactions locking the same rows in opposite order (a deadlock).
- **Transactions and resources**: writes that must succeed together made outside one transaction, a commit in the wrong place, a side effect (an HTTP call, a message) inside a transaction that may roll back, a connection, file, response or stream not closed on every path, an outgoing call without a timeout.
- **Contracts within the code**: a function that now returns, raises or accepts something else while its callers were not changed; a renamed field still read under its old name.

## Evidence

A finding names the scenario: "a request with `model=None` reaches `x.py:40`, which returns 200 with an empty body", "the call at `y.py:12` times out; nothing logs it and no metric counts it". Without a scenario you can state, it is a question to the author at most ("what happens when ...?"), and only when the code makes the case plausible.

## Not here

Slowness (Performance), attacks (Security), missing tests (Tests), API and schema compatibility and what passes between the old and the new version during a rollout (Compatibility), style (Design & Style).
