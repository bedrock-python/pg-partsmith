# AI review

The author of a pull request reviews the branch with an AI agent before asking people to review it, fixes what the review finds or dismisses it with a reason, and says so in the pull request's "AI Review" section. The people who review the pull request then look at what an AI cannot judge, and do not run it through an AI themselves ("Working with it" below).

It works the same in every agent: the same prompts, the same record. The reviewers always start in a fresh context, without the conversation that produced the change, on a model no weaker than the author's ("Fresh reviewers" in `review.md`). The agent that wrote the change only takes their findings and settles them with the user.

These files come from the `ai-review` pack of your organisation's engineering-assets hub.

| File | What |
|---|---|
| [`review.md`](review.md) | the review: scope, directions, verifying and ranking findings, the record |
| [`directions/`](directions/) | one prompt per direction: what to look for and what counts as evidence |
| [`pure-refactor.md`](pure-refactor.md) | a check that a change claimed to keep behaviour keeps it |
| [`check.py`](check.py) | says whether the branch's review covers its last commit; a pre-push hook runs it |

## Directions

| Direction | Looks for |
|---|---|
| [Correctness](directions/correctness.md) | logic errors, edge inputs, swallowed and unnoticed errors, races, transactions, resources |
| [Security](directions/security.md) | secrets, injection, access checks, data exposure, insecure defaults, new dependencies |
| [Performance](directions/performance.md) | N+1 and unbounded queries, blocking calls in async code, work that grows with the data |
| [Design & Style](directions/design.md) | why each new thing exists, what already exists, layers, simplicity, naming, comments |
| [Tests](directions/tests.md) | new behaviour without a test, tests that check nothing, tests bent to fit |
| [Compatibility](directions/compatibility.md) | API and schema contracts, migrations, rollout, stored data, configuration, flags, stale docs |

A repository adds its own focus areas in a `## Review` section of `.agents/project.md`, one line each: `- <direction>: <what to check>`. A name that is none of the directions above becomes a direction of its own, with its own line in the pull request.

## Running it

The agent runs the review by itself once the last change of a task is committed (`AGENTS.md`), so nobody has to remember. Unlike the starter review, it is not read-only: it writes the record in `local/`, and the agent then fixes the findings the user agrees with. By hand:

| Agent | Review | Pure-refactor check |
|---|---|---|
| Claude Code | `/review-change` | `/review-change pure-refactor` |
| Codex | `$review-change` | `$review-change pure-refactor` |
| Cursor | `/review-change` | `/review-change pure-refactor` |
| OpenCode | "review my branch" | "check that my branch keeps behaviour" |
| any chat | paste `review.md`, the direction files and `git diff <base>...HEAD` (`<base>` as in `review.md`) | paste `pure-refactor.md` and the diff |

The request may name directions (`/review-change security performance`), ask for `full` (the whole branch again, not a follow-up over the changes since the last review), or give `staged`, `working` or a path.

The skill is `.agents/skills/review-change/`, where Codex, Cursor and OpenCode find it. In Claude Code, `/review-change` of the `claude` pack follows it whenever this directory exists, and the `reviewer` subagent is the fresh reviewer it starts for each direction.

## The record

Every run updates the branch's one record, `local/reviews/<branch>.md` (`local/` is gitignored): the commit the last complete run covered, the reviewers' model, the result per direction and each finding with its status: fixed, dismissed with the reason, or open. The next run reviews only what changed since that commit and does not bring back what was dismissed. The format is in [`review.md`](review.md).

## In the pull request

The description ends with an "AI Review" section, built from the record and nothing else, in the language `.agents/project.md` names for pull requests. With the `claude` pack, `/pr` adds it. Put the section into the repository's pull request template, so that people see it is expected:

```markdown
#### AI Review

Commit `a1b2c3d`, Claude Opus 5.5

- [x] Correctness - 2 fixed
- [x] Security - no findings
- [x] Performance - 1 fixed, 1 dismissed
- [x] Design & Style - 3 fixed
- [x] Tests - no findings
- [x] Compatibility - not applicable: no API, schema or configuration change

<details><summary>Dismissed (1)</summary>

- `src/keys.py:42` - N+1 when loading keys: the list holds at most 5 keys
</details>
```

- The first line is the record's `commit` (short sha) and `model`. When the user declined a follow-up over later fixes, it adds ", fixes after it not reviewed again".
- One line per direction of the record, in its order and under its name, the repository's own directions and Pure-refactor check included. A direction that was not run is unchecked: `- [ ] Tests - not run`.
- The block of dismissed findings lists each dismissed blocking or should-fix finding with its reason; no dismissed findings, no block. Nits never go there, and a finding about a secret names its place and kind, never the value.

## Working with it

- **The AI review is the author's job.** Before the pull request is opened for review (a draft opened only so that CI runs is not), the author reviews the branch, fixes what the review finds or dismisses it with a reason, and fills the "AI Review" section. A pull request without it, or with its boxes unchecked, is not ready for review. After the fixes, and after changes made in review, the author runs a follow-up on the new commits: it checks the fixes and the lines they touched, not the whole branch again. Commits that only fix findings may go without one: the author's call, and the section says so.
- **People review what an AI cannot judge**: does the change solve the task, are the model and the contracts right, is the rollout plan right, what does it do to its neighbours, will the next person understand it, and are the dismissed findings fair.
- **No AI-generated comments on someone else's pull request.** Do not run a colleague's change through an AI and post what it says. Using an AI to understand the code is fine; the comment is your own.
- **A bug the AI review should have caught** gets a review thread that starts with `ai-miss:`. Besides the fix, the author turns the miss into a rule where the review reads it, and links it in the reply: a line in the `Review` section of `.agents/project.md` if it is about this repository, otherwise a pull request or an issue to the hub on `packs/ai-review/.agents/review/directions/`.

## Wiring

Optional, once per repository, in its own files, in a pull request merged after the sync pull request that brings `.agents/review/`: until then the hook finds no script and fails every push. `check.py` needs Python 3.9 or later and only its standard library, so it runs in a repository of any language.

- A pre-push hook in `.pre-commit-config.yaml`. It never blocks a push, since unfinished branches are pushed to run CI, and only says when the review is missing or behind the branch:

  ```yaml
  - repo: local
    hooks:
      - id: ai-review
        name: ai review
        entry: python .agents/review/check.py
        language: python
        pass_filenames: false
        always_run: true
        verbose: true
        stages: [pre-push]
  ```

- The way the repository installs its hooks installs `pre-push` too. Plain `pre-commit install` does once `default_install_hook_types` lists it (with `commit-msg` where a hook needs it); a README or Makefile that names hook types needs `--hook-type pre-push`. Where `pre-push` was not installed before, `default_stages: [pre-commit]` keeps the other hooks on commit instead of running them again on push; a repository whose hooks already run on push keeps them as they are. Hooks that declare their own stages, such as `trailing-whitespace` and `end-of-file-fixer` from pre-commit-hooks, run on push as well. After the change, everyone installs the hooks once more.
- Without pre-commit, a `.git/hooks/pre-push` that runs `python .agents/review/check.py` does the same for one clone, but it checks the checked-out branch, which a plain `git push` pushes; pre-commit tells it the branch being pushed.
