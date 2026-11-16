# Pure-refactor check

Run it on a refactoring before the pull request is opened, and on any branch whose description claims that behaviour does not change: a move, a rename, a dependency swap. How to run it in each agent is in [README.md](README.md).

Verify that the change on this branch keeps behaviour everywhere. Assume it does not until you have traced every path.

## Scope

`git diff <base>...HEAD` plus untracked files unless told otherwise, with `<base>` as [review.md](review.md) defines it. Read every changed file in full, old and new, and find every caller of every changed function, including references by string: registries, entry points, configuration, templates, dynamic lookups. Behaviour most often changes at a call site the diff does not show.

## Structure

Split the diff into slices small enough to trace completely: a module, a class, a group of related functions. Run one sub-agent per slice, all in parallel, started as "Fresh reviewers" in [review.md](review.md) says: fresh contexts, a model no weaker than this session's. Each sub-agent traces its slice only: it starts no sub-agents and edits nothing. If your agent has no sub-agents, the check runs in a new session as that section says, with the words "you are the fresh reviewer of `.agents/review/pure-refactor.md`: report only, edit nothing". Such a session is the reviewer itself: it starts no one, works through the slices one at a time, writing each slice's result down before the next, does the report step itself and returns the report; you add it to the record.

For every slice, compare the old code with the new, path by path, and answer with evidence:

- **Interface:** the same importable names and module paths, parameter names (callers may pass them by keyword), configuration keys and environment variables, command-line flags, routes, and names in stored data and events?
- **Inputs:** the same accepted arguments, types, defaults and validation? The same behaviour on `None`, empty and boundary values?
- **Outputs:** the same return values and types, the same shape and order of collections, the same serialised form (field names, aliases, missing versus null, number and date formats)?
- **Errors:** the same exceptions for the same conditions, the same error codes and messages reaching callers, the same log level and event?
- **Side effects:** the same writes to storage, the same outgoing requests (payload, headers, order, retries, timeouts), the same metrics and their labels, the same logging context?
- **Order and timing:** the same order of operations wherever it can be observed: transaction boundaries, when a lock is taken, when a stream yields, when a task starts?
- **Removed or added code:** does anything that ran before no longer run? Does anything run now that did not before (a validation, a default, a log, a conversion)?
- **Tests:** were tests changed? A changed assertion or fixture in a change that claims to keep behaviour is a behaviour change until shown otherwise.

A slice's result is either "no behaviour change - traced <what>" or a list of differences, each as `path:line - old behaviour → new behaviour - how it is observable`.

## Report

One more fresh sub-agent, as for the meta-review in [review.md](review.md), gets the slices' results, not your opinion of them. It merges them, verifies each difference at the cited lines, drops what it cannot confirm, and reports in priority order, naming the models the check ran on:

```
## Pure-refactor check: <branch>, <model(s)>

### Behaviour changes (blocking: the change does not keep behaviour)
- `path:line` - <old → new>. Observable as <a response / stored data / a log / an outgoing request / timing>.

### Probable changes (could not trace fully)
- `path:line` - <what differs> - <what would settle it>

### Verified unchanged
- <slice> - traced <interface / inputs / outputs / errors / side effects>

### Verdict
<keeps behaviour | behaviour changes found> - one line.
```

No compliments, no summary of the restructuring itself. If a difference is intended, say so and name where the author states it: an intended change nobody stated is still a finding.

## Record

Add the result to the branch's review record (`local/reviews/<branch>.md`, see "After the report" in [review.md](review.md)) as the direction "Pure-refactor check": "no behaviour change", or each difference as a finding with its status. A behaviour change is blocking; a probable change is should fix until it is traced. It is a partial run: it never moves the record's `commit`. The pull request's "AI Review" section then carries it as a line of its own.
