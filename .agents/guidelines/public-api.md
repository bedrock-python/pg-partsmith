# Public API guidelines

The public API is a promise to every user. Change it deliberately, announce it, and give people time.

## What is public

- Public is what the documentation describes and what is importable without a leading underscore from the package and its documented modules. Everything else is private: name it with an underscore, keep it out of `__all__` and out of the docs.
- Types, exceptions, default values, the order of positional parameters and the meaning of return values are all part of the API.
- Keep the public surface small. Adding is easy; removing takes a major release.

## Versioning

We follow semantic versioning:

- **patch** (1.4.2 → 1.4.3): bug fixes that change no documented behaviour;
- **minor** (1.4 → 1.5): new features and deprecations, compatible with existing code;
- **major** (1.x → 2.0): removals and incompatible changes, each one announced by a deprecation in an earlier minor release.

Before 1.0, a minor release may break the API; say so in the changelog.

## Deprecations

1. Keep the old API working and make it emit a `DeprecationWarning` (or the project's subclass of it) that names the replacement and the version that removes it, with `stacklevel` pointing at the caller.
2. Document the deprecation in the changelog and in the API reference.
3. Remove it only in the next major release, and no sooner than the deprecation period the project promises.

## Every change to the public API

- Update the API reference, the examples and the changelog in the same pull request.
- Update the pages written for AI agents, if the project has them (for example an agents page or `llms.txt` in the docs): they must describe the API as it is now.
- Add or update tests that pin the new behaviour.
- Mention in the pull request whether the change is a patch, minor or major change, and why.
- Prefer keyword-only parameters for new options (`def f(x, *, strict=False)`), so they can be added and reordered without breaking anyone.
