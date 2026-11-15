---
name: review-change
description: AI review of the current branch by directions - correctness, security, performance, design and style, tests, compatibility - or, with pure-refactor, a check that a change keeps behaviour. Run it yourself as soon as a task is done, before opening a pull request, and on request.
---

# Review a change

The review lives in `.agents/review/`; this skill only picks the prompt. Work from the repository root, the directory that holds `.agents/`.

`pure-refactor` in the request: [pure-refactor.md](../../review/pure-refactor.md); otherwise [review.md](../../review/review.md). Read it in full and follow it exactly, with the rest of the request as its scope and directions.
