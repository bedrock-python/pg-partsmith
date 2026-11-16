---
name: implement
description: Implement a task in the code - a bug fix, a feature or a refactoring - from understanding it to the report, with a review at the end. Use when the user hands over such a task, not for an edit within a task already under way.
---

# Implement

Work from the repository root, the directory that holds `.agents/`. The rules are in `AGENTS.md`, the repository's `CONTRIBUTING.md` if it has one, `.agents/project.md` and the guidelines in `.agents/guidelines/` for the areas the task touches: read them first. This skill adds only the order of the work and what they do not say.

## 1. Understand the task

1. Look for the same flaw elsewhere in the repository (the same cause, not only the same symptom) and propose one fix for all of it.
2. Find everything the change touches: callers, tests, docs, configuration, deployment values and the other services that read the contract.

## 2. Agree on the shape

1. A change beyond a few lines: before writing code, show its shape (the contract or public API, the files it adds or changes, a sketch) and wait for the answer. A spec agreed as `.agents/skills/spec/SKILL.md` says already gives the shape.
2. Where there is a choice, give the options with how the neighbouring code and other projects solve the same problem (read their code, not their docs), and say which one you prefer and why.
3. A fix or a refactoring the task needs first goes to a pull request of its own, merged before the main change.

## 3. Write the change

- Work on the branch the user names; on the default branch, create one first as `.agents/skills/new-branch/SKILL.md` says, in a worktree when another agent works in this clone.
- The same flaw elsewhere is part of the task once the user agreed to the wider fix in step 2.
- A bug fix starts with the test that reproduces it, run on the unfixed code: locally, or, when the tests run in CI, committed and pushed alone to the branch's pull request after the user's yes. A draft opened for that gets a title by the profile's format and a one-line description; `.agents/skills/pr/SKILL.md` writes the full one when the work is done.
- Configuration and tests change in the same step as the code they follow, not at the end.
- After a rename or a move, search for the old name in code, tests, docs and configuration.

## 4. Verify

1. Run the format, lint and test commands as `AGENTS.md` says. Where the tests run only in CI, or the user asked to leave them to CI, say so: they run in step 2 instead.
2. Commit as `.agents/skills/commit/SKILL.md` says. If the tests are left to CI, push after the user's yes to the branch's pull request (a draft if none is open yet) and read the pipeline.
3. Review the branch: with `.agents/review/`, follow `.agents/review/review.md` and settle the findings as it says; otherwise follow `.agents/prompts/review.md` and fix with the user what it finds.

## 5. Report

End with a short report that stands on its own: what changed, how it was verified (what you ran and what it printed), what is left and the next step.
