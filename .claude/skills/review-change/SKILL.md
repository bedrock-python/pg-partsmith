---
name: review-change
description: Review a change for bugs, security problems, compatibility, unjustified additions and missing tests, using the project's review checklist; with pure-refactor, check that a change keeps behaviour. Use when the user asks for a review, a second look at a diff, branch, commit range or pull request, or whether a refactoring keeps behaviour.
argument-hint: "[branch, commit range, files or pull request] [pure-refactor]"
---

Review the change that $ARGUMENTS names, leaving out the word `pure-refactor`. With nothing named, review the current branch against the default branch, plus uncommitted and untracked changes.

1. Pick the prompt: `.agents/prompts/pure-refactor.md` when the arguments say `pure-refactor` or the user asks whether behaviour is kept, `.agents/prompts/review.md` otherwise. Read it in full and follow it, including its report.
2. Read `.agents/project.md` for the project's conventions and default branch.
3. Split the change into slices as the prompt says. For a small change, one slice, review it yourself. For more, read only what splitting and the whole-change checks need (`git diff --stat`, the description, the docs it may affect) and give each slice to a `reviewer` subagent, all in parallel. Name in each request the prompt, the slice, and how to get the whole change: a range, or "uncommitted and untracked changes".
4. Merge what the subagents return. With `review.md`, do its whole-change step yourself. Check every finding at the line it cites, then report as the prompt says.

Do not edit files.
