# Compatibility

What does the change do to those who already depend on this code: the clients of its API, other services, the data already stored, the running deployments and those who keep them up, the people who read the docs? A change can be right and still break them on the day it ships.

## Look for

- **API and messages**: a field removed, renamed or retyped, a new required field, a changed default, status code or error format, a message or event schema others consume. Who consumes it: see the docs and the repository's notes for agents. If the change breaks a contract, ask whether the breaks already planned for it (work in progress in those notes, TODOs and deprecation notices on that contract) ship in the same release, each in a pull request of its own, so that its clients migrate once.
- **Database**: a migration without a working downgrade, a data migration that goes wrong on real data, a lock or a rewrite of a large table, `NOT NULL` without a default on a filled table, code that cannot run against both the old and the new schema while the rollout is under way. A migration over a large table is a question: how long does it take on data of production size? A downgrade left empty on purpose, with a warning, where the repository's guidelines allow it, is no finding.
- **Rollout**: the old and the new version run side by side during the rollout: a request, message or task made by one and handled by the other; a deploy order the change depends on, between services or with the migration: ask for it.
- **Stored and cached data**: keys, serialised objects and files written by the old version that the new one must still read, and the other way round during a rollback.
- **Configuration**: a new required setting without a default, an environment variable or flag renamed or removed, deployment values (Helm, Compose) not changed along with it.
- **Behaviour**: a risky change visible to users or other services without the feature flag the repository's guidelines ask for, unless the pull request says why it needs none. A flag on a change that is not risky is a finding too: it is a second code path for nothing.
- **Docs**: `docs/`, the README or the public docs that the change makes wrong; the changelog, if the repository keeps one.
- **Dependencies**: an upgrade whose changelog lists breaking changes the code still relies on.

## Evidence

Name who breaks and when: "the billing service reads `tier` from this response; after the rename it gets `null` until it is redeployed". If an order of rollout or a flag avoids it, say which. For a rollout, name the two versions and what passes between them: "an old worker reads the task the new one queued and fails on the field `priority`".

## Not here

Bugs inside the change and failures nobody would notice (Correctness), the security of a new contract (Security), a migration without a test (Tests).
