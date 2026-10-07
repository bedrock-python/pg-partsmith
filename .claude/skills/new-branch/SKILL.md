---
name: new-branch
description: Create a branch for a task, named by the project's convention.
disable-model-invocation: true
argument-hint: "<issue or short description>"
---

Create a branch for: $ARGUMENTS

1. Read the branch convention and the default branch in `.agents/project.md`. If it names no convention, use `<type>/<issue>-<short-description>` in lowercase with hyphens, such as `fix/123-empty-email`.
2. Fetch the default branch and start from its latest commit on the remote, unless the user names another base. If the working tree has uncommitted changes, ask whether they come along.
3. Switch to the new branch. Do not push it.

Finish with the branch name and the commit it starts from.
