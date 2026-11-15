---
name: new-branch
description: Create a branch for a task from the latest default branch (or the base the request names), named by the project's convention - type, issue, short description - in this clone or in a worktree of its own, so that agents can work on several tasks in parallel.
---

# New branch

Work from the repository root, the directory that holds `.agents/`. Read the branch convention, the issue format and the default branch in `.agents/project.md`; where it names none, find them as `AGENTS.md` says ("Before you start").

## Flow

1. **Where.** In this clone, unless the caller or the user asks for a worktree: a directory of its own on the new branch, so that another agent keeps working in this clone meanwhile.
2. **Uncommitted changes** (this clone only; a worktree leaves this clone as it is): if `git status` shows any, stop and say so: they have to be committed or stashed first.
3. **Base.** The branch the request names as the base, else the default branch. `git fetch origin`: the new branch starts from `origin/<base>`, so nothing else in this clone changes.
4. **Issue.** Ask the user for the issue, unless the request names it. Check it against the issue format in the profile; ask again if it does not fit. A repository without a tracker skips this step.
5. **Description.** Ask what the task is, unless the request says, and derive from it:
   - the type: `feat` (new functionality), `fix` (a bug), `perf` (speed or resources), `refactor` (restructuring without a behaviour change), `chore` (maintenance, dependencies, configuration), `test`, `docs`; ask when it is ambiguous;
   - a short description: three to five words, lowercase, with hyphens.
6. **Confirm.** Show the branch name by the profile's convention and, for a worktree, its path: next to this clone, `../<repo>-<short-description>` (`<repo>` is this clone's directory name), unless the profile or the user names another place. Create it only after the user's yes; change it as they say.
7. **Create.**
   - This clone: `git switch -c <branch> origin/<base>`.
   - Worktree: if the path exists, stop and say so. Otherwise `git worktree add -b <branch> <path> origin/<base>`; from then on every command runs in that directory. Tell the user: files git ignores (`.env`, `local/`) are not in the worktree, so copy from this clone what the app and the tests need; test infrastructure on the machine (containers, ports, a test database) is shared by every worktree, and two test runs on it at once break each other; once the pull request is merged, `git worktree remove <path>`.

## Convention

Without one in the profile: `<type>/<issue>-<short-description>`, such as `fix/123-empty-email`, or `<type>/<short-description>` without a tracker.

Do not push the branch. If a git command fails, show the error and stop. Finish with the branch name, the commit it starts from and, for a worktree, its path.
