# Before you refactor

Answer these questions before you change the structure of code. If an answer is "no" or "I don't know", stop and talk it through with the user first.

1. **Purpose.** What concrete problem does it solve: a bug that keeps coming back, a feature that is hard to add, code nobody can follow? "Cleaner" alone is not a reason.
2. **Safety net.** Do tests cover the behaviour you are about to move? If not, write tests that pin the current behaviour first, and commit them on their own.
3. **Behaviour.** Does anything observable change: results, error messages, logs, performance, the public API? A refactoring changes none of these; anything else is a feature change and is reviewed as one.
4. **Size.** Can it be done in steps that each leave the code working and can be reviewed and reverted on their own?
5. **Reach.** Does it stay inside the code the task is about? Shared modules, public interfaces and changes across many files need their owners' agreement.
6. **Timing.** Will it collide with open pull requests or a release in progress?

When you go ahead, keep refactoring commits apart from behaviour changes, run the tests after every step, and explain in the pull request what moved where and why behaviour is unchanged.
