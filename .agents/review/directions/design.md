# Design & Style

Is the change the simplest code that fits this repository? Read it the way a person reads a file, top to bottom, against `AGENTS.md`, the repository's `CONTRIBUTING.md` if it has one, every guideline in `.agents/guidelines/` for the areas the scope touches, and the conventions in `.agents/project.md`. You owe the change nothing: every addition has to justify itself in the diff.

## Look for

- **Why it exists**: of every new class, function, parameter, configuration key, flag, metric, test and dependency, ask what problem it solves now, who asked for it and where it is used (search for it). "Might be useful later" is a reason to remove it.
- **What already exists**: a helper, fixture, pattern or library function that does the same: find it and cite it.
- **Where it sits**: the right module and layer, the direction of imports, a decision made once rather than the same check at several levels.
- **How simple it is**: an abstraction with a single implementation, indirection without a second use, clever code where plain code reads better.
- **Special cases**: a fix that adds a hook or a branch for one object (`if provider == "x"`): how do the other objects here solve the same problem? One object's special case is often a general rule that is missing.
- **How it reads**: names that say what a thing is, consistency with the neighbouring code, comments only where the code cannot say why it is done this way or what breaks if it changes; a comment that retells the code is noise.

## Evidence

Cite the rule the finding rests on (`AGENTS.md`, `CONTRIBUTING.md` or the guideline, with its section) or the existing code, as `path:line`. Taste without either is no finding.

Besides the findings, list every new public thing in the scope with the reason found for it in the diff or its context, or "no reason found": the report's "Justify or remove".

## Not here

Bugs (Correctness), speed (Performance), what the tests cover (Tests).
