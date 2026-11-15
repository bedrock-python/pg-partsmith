---
name: pr
description: Write the pull request (merge request on GitLab) title and description for the current branch from its commits and diff, short and about why, with the AI Review section where the repository has the AI review; open it after the user's yes.
---

# Pull request

Work from the repository root, the directory that holds `.agents/`. `<base>` is the target branch the request names, else the default branch as the remote has it (`git symbolic-ref --short refs/remotes/origin/HEAD`, else `origin/master` or `origin/main`); the pull request targets it. Read `.agents/project.md` for the title format, the language of pull requests and how they refer to issues.

## Before writing

1. **Committed.** Everything belongs to the branch: no uncommitted changes. The project's checks pass, or the user knows which fail.
2. **Docs.** Check `git diff <base>...HEAD` against `docs/`, the README, the examples and the changelog: names renamed or removed but still mentioned, logic described differently, what users need to know and nobody wrote. With a large diff or large docs, give each area to a sub-agent in parallel, if your agent has them. A mismatch: stop and fix the docs on this branch first.
3. **Rules.** Check the diff against the rules for changes in `AGENTS.md` and the repository's `CONTRIBUTING.md`: one purpose, a size a person can review, an honest type, no files that do not belong. Where the guidelines ask for a feature flag on a risky change, check that it has one, and that a change that is not risky has none. On a violation, stop: say what to fix and write nothing.
4. **Review.** With `.agents/review/`: run `python .agents/review/check.py --strict --refresh` (`python3` where `python` is not on the path). Unless it reports the branch reviewed, review it as `.agents/review/review.md` says and settle the findings first. One exception: when only commits that the record lists as "fixed in" came after the review, ask whether to run a follow-up over them; if the user declines, the AI Review section says so. Stop while `local/reviews/<branch>.md` still has an open blocking or should-fix finding.

## Title

By the profile's format, in its language. Without one: `<type>(<issue>): <summary>`, with the issue from the branch name, or `<type>: <summary>` without a tracker. One main change, without versions or secondary details: release notes are built from titles. The type is honest: `perf` for an optimisation, `feat` or `fix` for anything users see, `chore` only for what they do not.

## Description

In the profile's language, by the repository's template if it has one (`.github/pull_request_template.md`, `.gitlab/merge_request_templates/`). Without one:

- the issue link, from the issue tracker in the profile or from the user; none without a tracker;
- **why**, in one or two sentences: the problem, not the list of changes;
- **what is included**: one short sentence per change, usually three to six; an API change shows an example request and response;
- only when it applies: the approach (the decisions the diff does not show, and what was rejected) for a larger change; measurements (one table, with the environment and the load) for a claimed effect on speed, memory or cost; the rollout (what breaks, the migration, the deploy order) when clients or operators must react or the schema changes;
- with `.agents/review/`: the "AI Review" section, built from the review record as `.agents/review/README.md` says.

Short: no sections beyond these, no retelling of the diff, no list of files, no linter output, no list of tests run. A check by hand the repository asks for, such as on a staging environment, is one line of what was verified. No AI attribution.

## Open it

Show the title and the description and wait for the user's yes. Then push the branch and open the pull request with the forge's command-line tool (`gh`, `glab` or `tea`), or give the text ready to paste. After changes made in review, write the description again, with the AI Review section after a follow-up.

Never merge, approve or assign reviewers unless asked.
