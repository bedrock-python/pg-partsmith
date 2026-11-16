---
name: spec
description: An interactive session that plans a task before any code - the problem first, then the code, then a spec saved where the project keeps specs, local/specs/ by default.
---

# Spec

Work from the repository root, the directory that holds `.agents/`. An interactive session that ends in a spec. No code is written in it: only questions, reading and planning. It goes on until the user says it is done ("save", "done", "that's it").

## 1. What: the problem

1. Ask what the task is, unless the request says; let the user describe it freely.
2. Ask, rather than assume, until it is clear: the problem it solves and for whom, the behaviour expected, the edge cases and errors, the constraints and dependencies.
3. Agree on the scope: what is in, and what is explicitly out.
4. Sum it up and ask whether it captures the task. Move on only on a yes; with new requirements or a "not quite", stay here.

If the user wants to skip to the code, bring them back to the problem first. When the discussion stalls, sum up what is clear and ask what is missing.

## 2. How: the plan

5. Read the code: the files involved, the patterns to follow, what to create or change. Use sub-agents if your agent has them.
6. Build the plan step by step, with the files you found. Where there is a choice, give the options and your preference.
7. Go through it with the user and change it as they say. The user may go back to step 1 at any time ("wait", "actually..."): follow.

## 3. Save

8. When the user says the plan is complete (if unsure, ask whether to save), offer two or three names for the spec, such as `auth-refactor`, and take theirs if they type one.
9. Save it where `.agents/project.md` says specs go. Without a place there, a spec is a working note, not a repository document: `local/specs/<name>.md`. If git does not ignore that path (`git check-ignore -q local/specs/<name>.md` fails), ask where to save it instead.
10. Show the path and sum up what the spec holds.

```markdown
# Spec: <name>

## Context
Why this is needed, the problem it solves.

## Task
What has to be done: the requirements from step 1.

## Out of scope
The boundaries agreed in step 1.

## Plan
Step by step, with the files from step 2.

## Success criteria
Testable statements ("the tests for X pass") and what a user can do afterwards.
```

Keep it balanced: concrete enough to work from, short enough to read in a few minutes. Ask open questions in chat; offer choices only when there are two to four concrete ones.

Do not start implementing until the user approves the spec.
