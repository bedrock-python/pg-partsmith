# Review a change

Review the change as a careful senior engineer of this project would. You are reviewing, not rewriting: do not edit files unless you are asked to.

## What to review

The change you were given: a pull request, a branch, a commit range or a set of files. If none was named, review the current branch against the default branch, plus uncommitted changes. Read enough of the surrounding code to judge each change in its context, and read `.agents/project.md` for the project's conventions.

## What to look for, most important first

1. **Correctness.** Does the change do what its description says? Check edge cases, error paths, empty and missing inputs, concurrency, time zones and encodings.
2. **Security.** Input validation, injection, authorisation, secrets in code or logs, unsafe deserialisation, new dependencies and what they can do.
3. **Compatibility and data.** Public APIs, configuration and file formats, database migrations, rollout and rollback.
4. **Tests.** Would a test fail without the change? Are the important cases covered? Do the tests check behaviour rather than implementation details?
5. **Operations.** Logging, metrics, error messages a person can act on, timeouts and retries.
6. **Readability.** Names, structure, duplication, dead code, comments that explain why rather than what.

Skip style points that a formatter or linter already enforces.

## How to report

Group findings by severity:

- **Must fix**: bugs, security problems, data loss, broken compatibility.
- **Should fix**: likely problems, missing tests, confusing code.
- **Consider**: suggestions and questions.

For each finding give the file and line, what is wrong, why it matters, and a concrete fix. If you are not sure, say so and say what would settle it. End with a one-line verdict. If you found nothing serious, say so plainly: do not invent findings to fill the list.
