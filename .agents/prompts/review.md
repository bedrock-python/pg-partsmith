# Review a change

Review the change as a careful senior engineer of this project would. You are not the author and you owe the change nothing: every addition has to earn its place. You are reviewing, not rewriting: do not edit files unless you are asked to.

## What to review

The change you were given: a pull request, a branch, a commit range or a set of files. If none was named, review the current branch against the default branch, plus uncommitted and untracked changes. Read `.agents/project.md` for the project's conventions, and the guidelines in `.agents/guidelines/` that cover the code the change touches.

Read every changed file in full, not only the changed lines, and look up the callers of every function whose behaviour changed. Most problems are in what the change does not fit with: a helper that already exists, a pattern the rest of the module follows, a caller that now breaks. Search for them and cite where they are instead of guessing.

## How to work through it

1. **Split the change into slices**, each small enough to read completely: a module or a package with its tests. A small change is one slice.
2. **Review each slice** against the list below. If your tool can run sub-agents, give each slice to one and run them in parallel; otherwise work through the slices one at a time and write down each slice's findings before you start the next. Tell each sub-agent this prompt, its slice and how to get the whole change (a range, or the uncommitted and untracked changes). A sub-agent given one slice does steps 2 and 4 for that slice only; steps 1 and 3 stay with whoever split the change.
3. **Review the change as a whole**, for what no slice can see:
   - a second task mixed into the change, or files that do not belong in it (generated output the project does not commit, local notes, debugging leftovers);
   - documentation, examples and the changelog that the change makes wrong;
   - the same helper written twice, one idea under two names, an abstraction with a single implementation and no test double;
   - behaviour that changes although the description says it does not.
4. **Check every finding** at the line you cite. Drop what you cannot confirm, merge duplicates, and list repeats once with their other locations.

## What to look for, most important first

1. **Correctness.** Does the change do what its description says? Check edge cases, error paths, empty and missing inputs, concurrency, time zones and encodings. An unexpected input or state must fail loudly, not fall back to a default nobody will notice.
2. **Security.** Input validation, injection, authorisation, secrets in code or logs, unsafe deserialisation, new dependencies and what they can do.
3. **Compatibility and data.** Public APIs, configuration and file formats, database migrations, rollout and rollback.
4. **Tests.** Would a test fail without the change? Are the important cases covered? Do the tests check behaviour rather than implementation details?
5. **Necessity.** For every addition, meaning a new public name, parameter, option, configuration key or dependency: what problem does it solve now, and where is it used? "It might be useful later" is a reason to leave it out.
6. **Operations.** Logging, metrics, error messages a person can act on, timeouts and retries.
7. **Readability.** Names, structure, duplication, dead code, comments that explain why rather than what.

Skip style points that a formatter or linter already enforces. Problems in lines the change adds or modifies are not excused because the code "already was like that"; problems elsewhere in a changed file are at most Consider.

## How to report

Group findings by severity:

- **Must fix**: bugs, security problems, data loss, broken compatibility.
- **Should fix**: likely problems, missing tests, confusing code.
- **Consider**: suggestions and questions.

For each finding give the file and line, what is wrong, why it matters, and a concrete fix. Where the author has to decide, ask the question they must answer ("Where is this option used?", "What happens when the list is empty?"). If you are not sure, say so and say what would settle it.

Then add **Justify or remove**: every addition (see Necessity), each with the reason you found in the change or its description, or "no reason found". Additions are judged here only, not again among the findings.

End with a one-line verdict: ready, ready after the "should fix" items, or needs rework. If you found nothing serious, say so plainly: do not invent findings to fill the list.
