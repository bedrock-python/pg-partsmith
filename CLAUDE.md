# CLAUDE.md

The instructions for every coding agent are in AGENTS.md, and the facts about this project in .agents/project.md. Both are imported below; keep anything Claude-specific short.

@AGENTS.md
@.agents/project.md

## Claude Code

- Skills: `/review-change` for a review; `/spec` before a larger change; `/new-branch`, `/commit` and `/pr`, which change the repository or the forge, run only when you invoke them.
- The `reviewer` subagent reviews a large change in a context of its own.
- `.claude/settings.json` only denies access to secrets. Keep your own permissions in `.claude/settings.local.json`, which is not committed.
