# Check that a change keeps behaviour

Use this on a change that claims to change no behaviour: a refactoring, a move, a rename, a dependency swap. Assume it does change behaviour until you have traced every path. You are checking, not rewriting: do not edit files unless you are asked to.

## What to check

The change you were given; if none was named, the current branch against the default branch, plus uncommitted and untracked changes. Read every changed file in full, old and new, and find every caller of every changed function, including references by string: registries, entry points, configuration, templates, dynamic lookups. Behaviour most often changes at a call site the diff does not show.

## How to work through it

Split the change into slices small enough to trace completely: a function, a class, a group of related functions. If your tool can run sub-agents, give each slice to one and run them in parallel; otherwise work through them one at a time and write down each result before the next. Tell each sub-agent this prompt, its slice and how to get the whole change (a range, or the uncommitted and untracked changes); a sub-agent given one slice traces that slice only, and whoever split the change merges the results.

For every slice, compare the old code with the new, path by path:

- **Interface:** the same importable names and module paths, parameter names (callers may pass them by keyword), configuration keys and environment variables, command-line flags, routes, and names in stored data and events.
- **Inputs:** the same accepted arguments, types, defaults and validation; the same behaviour on `None`, empty and boundary values.
- **Outputs:** the same return values and types, the same order of collections, the same serialised form (field names, missing versus null, number and date formats).
- **Errors:** the same exceptions for the same conditions, the same error codes and messages reaching callers, the same log level and event.
- **Side effects:** the same writes to storage, the same outgoing requests (payload, headers, order, retries, timeouts), the same metrics and their labels.
- **Order and timing:** the same order of operations wherever it can be observed: transaction boundaries, when a lock is taken, when a stream yields, when a task starts.
- **Code that appeared or disappeared:** a check, a default, a conversion or a log line that runs now and did not before, or the other way round.
- **Tests:** a changed assertion or fixture in a change that claims to keep behaviour is a behaviour change until shown otherwise.

## How to report

- **Behaviour changes**: each as `file:line`, the old behaviour, the new one, and how a caller, a user or the stored data would notice.
- **Not fully traced**: what differs and what would settle it.
- **Unchanged**: each slice and what you traced in it.

End with a one-line verdict: behaviour kept, behaviour changes found, or not settled (and how many paths are not fully traced). A change the author intended still counts if the description does not say so.
