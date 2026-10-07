---
name: pr
description: Open a pull request (a merge request on GitLab) for the current branch, described from its commits and diff.
disable-model-invocation: true
argument-hint: "[target branch, or notes for the description]"
---

Open a pull request for the current branch. $ARGUMENTS

1. Read `.agents/project.md` for the default branch, the language of pull requests and how they refer to issues.
2. Make sure everything is committed and the project's checks pass. Report what fails and ask before going on.
3. Write the title in the project's format. Write the description from the commits and the diff: what changes and why, how it was tested, risks and how to roll back, and the issue it resolves. Fill in the repository's template if it has one (`.github/pull_request_template.md`, `.gitlab/merge_request_templates/`).
4. Show the title and description and wait for the user's approval. Then push the branch and open the pull request with the forge's command-line tool (`gh`, `glab` or `tea`), or tell the user how to open it.

Never merge, approve or assign reviewers unless asked.
