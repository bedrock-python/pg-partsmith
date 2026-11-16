---
name: pr
description: Open a pull request (a merge request on GitLab) for the current branch, described from its commits and diff.
disable-model-invocation: true
argument-hint: "[target branch, or notes for the description]"
---

If the repository has `.agents/skills/implement/` (the `workflow` pack), follow `.agents/skills/pr/SKILL.md` with the request "$ARGUMENTS" instead of the steps below.

Open a pull request for the current branch. $ARGUMENTS

1. Read `.agents/project.md` for the default branch, the language of pull requests and how they refer to issues.
2. Make sure everything is committed and the project's checks pass. Report what fails and ask before going on. If the repository has `.agents/review/`, run `python .agents/review/check.py --strict --refresh` (`python3` where `python` is not on the path); unless it reports the branch reviewed, run `/review-change` and settle its findings first, and stop while the review record (`local/reviews/<branch>.md`) still has an open blocking or should-fix finding.
3. Check that the documentation is still true: search the docs, the README, the examples and the changelog for what the branch renames, removes or changes, and for what it adds that users will need to know. If something is stale or missing, say what and offer to fix it on this branch; after a fix is committed, go back to step 2.
4. Write the title in the project's format. Write the description from the commits and the diff: what changes and why, how it was tested, risks and how to roll back, and the issue it resolves. Fill in the repository's template if it has one (`.github/pull_request_template.md`, `.gitlab/merge_request_templates/`). With `.agents/review/`, end the description with the "AI Review" section, built from the review record as `.agents/review/README.md` says.
5. Show the title and description and wait for the user's approval. Then push the branch and open the pull request with the forge's command-line tool (`gh`, `glab` or `tea`), or tell the user how to open it.

Never merge, approve or assign reviewers unless asked.
