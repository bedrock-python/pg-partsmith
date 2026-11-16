---
name: commit
description: Commit changes by the project's conventions - the docs checked against the diff, the diff against the rules, the formatter run, the message confirmed by the user.
---

# Commit

Work from the repository root, the directory that holds `.agents/`. Commit what the request names; with nothing named, what is staged; if nothing is staged, the changes to tracked files, after asking about new files.

## Flow

1. **Changes.** `git status` and the diff of what goes into the commit. Nothing to commit: say so and stop.
2. **Docs first.** Check the diff against `docs/`, the README and the examples: names renamed or removed but still mentioned, logic described differently, new concepts and changed contracts, configuration, commands or processes not documented. With a large diff or large docs, give each area to a sub-agent in parallel, if your agent has them. A mismatch: fix the docs first and commit them together with the code.
3. **Rules.** Walk the diff against `AGENTS.md`, the repository's `CONTRIBUTING.md` if it has one and the guideline in `.agents/guidelines/` for each area it touches. Stop and report instead of committing when it breaks them, in particular: files that do not belong in the repository (local notes, generated output, secrets), a second task in the same diff, debugging leftovers.
4. **Format.** Run the format command from `.agents/project.md` on what you commit; if the profile and the repository name none, ask. Report what the lint command reports, if there is one.
5. **Message.** By the commit convention in the profile. Without one, Conventional Commits: `<type>(<scope>): <summary>` in the imperative, at most 72 characters, with the issue from the branch name as the scope where the project does that, and a body that says why when the summary cannot. Types: `feat`, `fix`, `perf` (an optimisation is `perf`, not `feat`), `refactor`, `test`, `docs`, `chore` (never for a change that belongs in release notes).
6. **Confirm.** Show the message and the files, and commit only after the user's yes; change it as they say.
7. **Commit.** Do not amend, rebase or push unless asked. No AI attribution: no `Co-Authored-By` for an agent, no "Generated with" lines.

Finish with the commit's short hash and subject.
