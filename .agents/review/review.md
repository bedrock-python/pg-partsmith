# Review

You are reviewing a branch for its author before it becomes a pull request. The author owns the outcome: each finding is fixed or dismissed with a reason, and the pull request says which. The people who review the pull request will not hunt for what you were meant to find, so miss nothing you can confirm and report nothing you cannot.

The rules are in `AGENTS.md`, in the repository's `CONTRIBUTING.md` if it has one, in `.agents/guidelines/` where the repository has them and in `.agents/project.md`. This prompt does not repeat them; the files in `directions/` say how to apply them.

## Scope

`<base>` is the repository's default branch as the remote has it: `git symbolic-ref --short refs/remotes/origin/HEAD`, else `origin/master` or `origin/main`, whichever exists. `<record>` is `local/reviews/<branch>.md`, one per branch: every run updates it, none starts a new one.

- By default, the changes since the last review. First run `python .agents/review/check.py --refresh` (`python3` where `python` is not on the path): after a rebase that left the reviewed changes as they were, it moves the record's `commit` to `HEAD`. Then, if `<record>` exists and its `commit` is an ancestor of `HEAD`, the scope is the files the branch changed since then, those of `git diff --name-only <commit> HEAD` that are also in `git diff --name-only <base>...HEAD`:
  - no merge of `<base>` since `<commit>` (`git log --merges <commit>..HEAD` is empty): `git diff <commit> HEAD -- <files>`;
  - a merge of `<base>` since: `git diff <base>...HEAD -- <files>`, so that other people's changes in the same files stay out of the review.

  Otherwise the whole branch, `git diff <base>...HEAD`. Untracked files belong to the scope either way. Nothing changed since the last review: say so and stop.

  A run over the changes since the last review is a follow-up: it checks that the findings in `<record>` are settled in the code and raises new findings, blocking and should fix only, no nits, only on the lines changed since `<commit>` (those of `git diff <commit> HEAD -- <files>` that are in the scope) and on the code whose behaviour they change, such as an unchanged caller of a changed function. The rest of the files is read for context, not reviewed again: otherwise every round finds something new and the rounds never end.
- `full` in the request: the whole branch, still with the findings already in `<record>`.
- `staged`: `git diff --staged`; `working`: `git diff` and `git status --short`; a path: that file or directory against `<base>`.
- A commit range, another branch or someone else's pull request: that diff, reviewed the same way, but the run writes no record and fixes nothing: it is not the author's review of their own branch, and what it finds goes to the user only.
- Leave out what nobody reviews: lock files, generated and vendored code, minified bundles, binary files. Name what was left out in the report.

Read every changed file in full, not just the hunks, together with the callers, tests and helpers next to it: most findings are about what the change does not fit with. Search and cite the location instead of guessing.

## Directions

The files in `directions/`, listed in the table of [README.md](README.md), and the `Review` section of `.agents/project.md`, if the repository has one: a line `- <direction>: <what to check>` adds a focus area to that direction; a name that is no direction here makes a direction of its own. A request that names directions runs only those.

First decide, for each direction, whether the scope gives it anything to look at: a change to docs only gives Performance nothing; a change that touches no API, schema, configuration, stored format or documented behaviour gives Compatibility nothing. Such a direction is "not applicable", with the reason in a few words, and is not run. When unsure, run it.

## Fresh reviewers

The review is done in fresh contexts, never in the session that wrote the change: a reviewer who knows why the code was written this way stops seeing what is wrong with it. You, the agent of that session, only start the reviewers, take their report and settle it with the user ("After the report").

If your request says that you are the fresh reviewer of this prompt, you are not that agent: start no reviewers, review every applicable direction yourself, one after another, then do the meta-review, and return the report and the record's content. Edit nothing and write no record; skip "After the report".

- One fresh sub-agent per direction, all in parallel; for a small scope, a few dozen changed lines, one fresh sub-agent for all directions. Never a fork or any agent that inherits this conversation. In Claude Code, the `reviewer` subagent of the `claude` pack is such a reviewer.
- Every reviewer, the meta-review included, runs on this session's model or a stronger one: a fresh context, not a weaker model. Agents may hand sub-agents a lighter model of their own accord (a built-in agent, a default model for sub-agents in the settings), so name the model when you start each reviewer. Each reviewer names in its report the model it ran on, and the record's `model` is taken from these reports, never from your own. A reviewer that ran on a weaker model is named: in `model` by the run that sets it, next to its direction's result by a partial run.
- No sub-agents in your agent: ask the user to run the review in a new session of their agent, on this session's model (a new chat, `claude -p --model <model>`, `codex exec -m <model>`, `opencode run -m <provider>/<model>`), with the same request and the words "you are the fresh reviewer of `.agents/review/review.md`: report only, edit nothing". That session did not write the change, so it is the reviewer itself (above); you write `<record>` from the report and the record's content it returns.
- A reviewer's brief holds only what is listed below. Not what the change is for, how it was built, what was tried, which findings you expect or which you think are settled: the reviewer reads the code as the pull request's reviewer will.

Each direction gets:

- its file from `directions/`, `AGENTS.md`, `CONTRIBUTING.md` if the repository has one, `.agents/project.md` and the guidelines in `.agents/guidelines/` for the areas the scope touches;
- the scope as the git command that produces it, not a copy of the diff;
- for a follow-up, that it is one, with its rule from "Scope" and the command for the lines changed since `<commit>`;
- its focus areas from `.agents/project.md`;
- the findings already in `<record>` with their status. A dismissed finding comes back only when the code it was about changed or there is new evidence; a fixed one only when the fix does not hold; an open one whenever it is still there;
- the report format and the severities below;
- that it reviews this direction only: it starts no reviewers, edits nothing and writes no record, whatever `AGENTS.md` says about running a review;
- the request to end its report with the model it runs on.

A scope too large to read completely at once is reviewed module by module within the direction, writing the findings down after each module.

A direction reports each finding as `path:line - defect or question - why it matters - fix`, with its severity (blocking, should fix or nit), and ends with one line: how many findings, or "no findings - checked <what>".

## Meta-review

One more fresh sub-agent (the same one for a small scope) gets the scope and the directions' findings with the models they named, not your opinion of them, names its own model too, and then:

- Reviews what no direction sees: the diff as a whole against the rules for changes in `AGENTS.md` and the repository's `CONTRIBUTING.md`: a second task in the diff, files that do not belong (`local/`, debug leftovers, generated files, agent reports), unrelated formatting.
- Verifies each finding against the code and drops what it cannot confirm at the cited line. A finding without the evidence its direction asks for (a failure scenario, an attacker, a scale, a rule) is dropped too. In a follow-up, so are nits and findings outside what its rule in "Scope" allows.
- Drops a finding that needs a particular configuration only when it can see every place that sets it (deployment values, the repository's notes for agents) and none does, and neither the change nor its flag turns it on; it says where it checked. When the setting may come from a place it cannot read, such as a secret store or CI variables, it keeps the finding as a question to the author about its value there.
- Merges duplicates: the same defect found by two directions is one finding, under the direction its fix belongs to. Groups repeats ("same in 6 more places: a.py:10, b.py:22, ...").
- Ranks by consequence, not by direction. Data lost or corrupted, a security hole, a swallowed exception, a layer leak, new behaviour without a test, a risky change without a flag, a breaking change without a plan: blocking. A real problem of limited reach: should fix. Naming, wording, formatting: nit.
- Treats "it already was like that" as no defence for code the diff touches, and as a reason to leave code the diff does not touch alone.

## Report

```
## AI review: <branch> at <short commit> (<whole branch | since <short commit> | ...>), <model(s)>

### Blocking
- Correctness · `path/to/file.py:42` - <question or defect in one sentence>. <Why it matters>. → <fix>

### Should fix
- ...

### Nits
- `nit:` Design & Style · `path:line` - ...

### Justify or remove
- `ClassName` (file) - <reason | no reason found>

### Directions
- Correctness - 1 blocking, 1 should fix
- Security - no findings
- Performance - not applicable: docs only

### Verdict
<ready | ready after "should fix" | needs rework> - one line on why.
```

- Every finding has a location and a fix.
- Phrase it as a reviewer would: "why do we need X?", "where is this used?", "what happens when Y is None?", a question the author answers with code or with a removal.
- No compliments, no retelling of what the change does. Left-out files, if any, go on one line under the title.

## After the report

Only for the author's review of the checked-out branch: not for a range, another branch or a pull request ("Scope"), and not for a fresh reviewer.

From here on it is you, the agent that wrote the change: you know why it is the way it is, and you judge what in the report is valid. The user decides what is dismissed; you do the work.

1. Update `<record>` right after the report, or create it with its directories; if git does not ignore it (`git check-ignore -q <record>` fails), stop and tell the user instead. The new findings start as "open".
2. Fix the blocking and should-fix findings, except where the fix is a choice the user should make. Ask the user about those and about every finding you think is wrong. A dismissal needs the user's yes and a reason in one line that someone else can judge ("the list holds at most 5 keys", not "not needed"). Nits: fix them or let them go; a nit let go is never listed as dismissed. Update each finding's status in `<record>` as it is settled.
3. Once the fixes are committed, run one follow-up (see "Scope") unless the user says the fixes need none: it checks the fixes and the lines they touched. Whether another round follows the follow-up's own fixes is the user's call: report what it found and ask, never loop on your own.

The record:

```markdown
---
commit: <full sha of the HEAD the last complete run covered>
model: <the reviewers' model in the run that set commit, or unknown>
---

| Direction | Result |
| --- | --- |
| Correctness | 2 fixed |
| Security | no findings |
| Performance | 1 fixed, 1 dismissed |
| Design & Style | 3 fixed |
| Tests | no findings |
| Compatibility | not applicable: no API, schema or configuration change |

1. Blocking · Correctness · `path:line` - <finding> → <fix>
   Status: fixed in <short sha>
2. Should fix · Performance · `path:line` - <finding> → <fix>
   Status: dismissed - <reason the user gave>
3. Nit · Design & Style · `path:line` - <finding> → <fix>
   Status: let go
```

- `commit` is the `HEAD` a complete run reviewed: a default or `full` run of every applicable direction. Only such a run sets it, when it writes the record after its report; fixes and status changes never move it, so the next run reviews the fixes. A partial run (named directions, `staged`, `working`, a path, the pure-refactor check) adds its findings and results but leaves `commit` as it is; on a branch without a record it writes one without `commit`.
- `model` goes with `commit`: the run that sets one sets the other, to the model its reviewers named, or for reviewers on different models `Claude Opus 5.5; Security on Claude Sonnet 5.5`.
- Directions in the order of the table in [README.md](README.md), then the repository's own, then Pure-refactor check if it ran.
- A result is "no findings", "not applicable: <reason>", "not run" (left out by the request) or the counts of fixed, dismissed and open findings over all runs; nits let go are not counted. A later "not applicable" or "not run" never replaces counts or "no findings"; "not applicable" replaces "not run".
- Findings keep their numbers from run to run; new ones are appended. A status is "open", "fixed in <short sha>", "dismissed - <reason>" or, for a nit only, "let go".
- A finding about a secret names where it is and what kind it is, never its value: the record is kept, and the dismissed findings go to the pull request description.

`python .agents/review/check.py` compares the record's `commit` with the branch; the pull request's "AI Review" section is built from the record ([README.md](README.md)).
