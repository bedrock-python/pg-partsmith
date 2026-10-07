---
name: review-change
description: Review a change for bugs, security problems, compatibility and missing tests, using the project's review checklist. Use when the user asks for a review or a second look at a diff, branch, commit range or pull request.
argument-hint: "[branch, commit range, files or pull request]"
---

Review $ARGUMENTS by following `.agents/prompts/review.md`. With nothing named, review the current branch against the default branch, plus uncommitted changes.

- Read `.agents/project.md` first for the project's conventions and default branch.
- Do not edit files. Report the findings as the checklist says, grouped by severity.
- For a large change, hand the review to the `reviewer` subagent and pass on its findings.
