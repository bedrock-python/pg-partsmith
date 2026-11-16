# Tests

Would the tests catch it if the change broke? Read the tests the change adds or alters together with the code they cover, and `.agents/guidelines/testing.md` where the repository has it.

## Look for

- **Behaviour without a test**: every new or changed behaviour, the error paths and the edge cases the code handles included, has a test that fails without the change: revert it in your head. A bug fix has a test that reproduces the bug; one that mocks out the code the fix changed, or asserts what the old code did as well, passes without the fix and reproduces nothing. A new migration is run by the repository's migration tests (upgrade, downgrade, upgrade again), where it has them.
- **What a test checks**: behaviour through the public interface rather than private details; an assertion on the result rather than only "no exception" or `assert result`; the exact value wherever it is known.
- **Changed tests**: an assertion, fixture or expected value changed in an existing test: is the new behaviour intended and said so? Otherwise the test was bent to fit the code.
- **Setup**: existing fixtures and factories reused rather than copied; no real network calls; nothing that depends on the time, the order of tests, randomness or sleeps.

## Evidence

Name the behaviour without a test and the case to add: "`x.py:40` returns 409 on a duplicate name; no test sends a duplicate". Code without logic of its own needs no test of its own.

## Not here

Whether the code itself is right (Correctness).
