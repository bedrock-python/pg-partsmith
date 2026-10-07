---
name: commit
description: Commit changes with a message that follows the project's conventions.
disable-model-invocation: true
argument-hint: "[what to commit, or a hint for the message]"
---

Commit $ARGUMENTS. With nothing named, commit what is staged; if nothing is staged, ask what to include.

1. Read the commit conventions in `.agents/project.md`. If it names none, use Conventional Commits: `type(scope): summary` in the imperative, at most 72 characters, and a body that says why.
2. Look at `git status` and the staged diff. Never commit secrets, build output or unrelated changes; tell the user what you left out.
3. If the profile names format or lint commands, run them on the files you commit and report failures before committing.
4. Write the message from the diff itself. Refer to the issue when the branch name or the user gives one.
5. Commit. Do not amend, rebase or push unless asked.

Finish with the commit's short hash and subject.
