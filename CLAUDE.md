# CLAUDE.md

The instructions for every coding agent are in AGENTS.md, and the facts about this project in .agents/project.md. Both are imported below; keep anything Claude-specific short.

@AGENTS.md
@.agents/project.md

## Claude Code

- Skills: `/review-change` for a review, `/review-change pure-refactor` to check that a change keeps behaviour; `/spec` before a larger change; `/new-branch`, `/commit` and `/pr`, which change the repository or the forge, run only when you invoke them. With the `workflow` pack (`.agents/skills/implement/`) they follow the skills of the same name in `.agents/skills/`, and `/implement` takes a task from understanding it to the report. With `.agents/knowledge/`, `/knowledge` reads, verifies and records the repository's knowledge.
- The `reviewer` subagent reviews a change, or one slice of a large one, in a context of its own; `/review-change` runs one per slice in parallel. With `.agents/review/` (the `ai-review` pack), `/review-change` runs one per direction instead and keeps a review record, and `/pr` adds its "AI Review" section.
- `.claude/settings.json` only denies access to secrets. Keep your own permissions in `.claude/settings.local.json`, which is not committed.
