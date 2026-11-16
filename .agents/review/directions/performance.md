# Performance

Will the change be slow, heavy, or hold others up under the load it will see? Judge against reality: how big the data is and how often the path runs. "Optimise only with a measurement in hand" holds for the review too: a finding states the scale that makes it matter.

## Look for

- **Database**: a query in a loop (N+1), a query without a limit or pagination on a table that grows, a filter or an order without an index (check the migrations), whole rows or tables loaded where a few columns or a count would do, a long transaction holding locks while it calls out.
- **Async code**: blocking calls on the event loop (a synchronous HTTP or database client, file IO, `time.sleep`, heavy parsing or hashing of large payloads); independent awaits run one after another on a hot path; concurrency without a bound (`gather` over input of any size).
- **Algorithms and memory**: quadratic work over collections that can grow, the same work repeated in a loop, a whole stream or file read into memory, a cache, queue or list without a bound, a client or connection created per request instead of reused.
- **Network**: calls in a loop that the other side can take as one batch, a payload far larger than its consumer needs.

## Evidence

A finding says how big or how often: "`list_keys` runs a query per service; a tenant has up to 2 000 services, so one page load makes 2 000 queries". Without a scale that makes the cost real, it is no finding; a micro-optimisation is a nit at most.

## Not here

Whether the result is right (Correctness), locks a migration takes during a rollout (Compatibility).
