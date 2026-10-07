---
name: spec
description: Write a short technical spec for a feature or change before coding it.
disable-model-invocation: true
argument-hint: "<feature, change or issue>"
---

Write a spec for: $ARGUMENTS

1. Read `.agents/project.md` and the code the change will touch. Ask the user for what the repository cannot tell you: goals, constraints, deadlines.
2. Write the spec in Markdown, two pages at most:
   - **Problem**: what is wrong or missing, for whom, and how we know.
   - **Goals and non-goals.**
   - **Proposal**: the design, the interfaces and data that change, with examples.
   - **Alternatives** considered, and why they lost.
   - **Risks**: compatibility, security, performance, rollout and rollback.
   - **Test plan.**
   - **Open questions.**
3. Save it where the profile says specs go. If it says nothing, propose `docs/specs/<yyyy-mm-dd>-<short-name>.md` and ask before writing.

Do not start implementing until the user approves the spec.
