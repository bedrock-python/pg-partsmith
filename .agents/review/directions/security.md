# Security

Can someone read, change or break what they should not through this change, or does it leave a secret where it does not belong? For each input the change handles, ask who controls it: a client, another service, an operator's configuration, a file from outside.

## Look for

- **Secrets**: a key, token, password or connection string in code, configuration, tests, fixtures, logs, error messages or URLs; a secret given a default value; a new setting that holds a secret but is not handled as one the way the repository handles the others.
- **Injection**: SQL, shell commands or templates built from strings; a file path joined from input (traversal); a URL fetched from input (SSRF); `pickle`, `yaml.load`, `eval` and the like on data from outside; a regular expression on input that can backtrack for long.
- **Access**: a new endpoint, handler or job without the authentication its neighbours have; an object read or changed by id without checking that it belongs to the caller; a check on one path and not on another that reaches the same data; a role or privilege taken from input.
- **Exposure**: personal data, tokens or keys in logs, metric labels or traces; a response or an export carrying internal fields; a stack trace or internal error text sent to a client; a new table, column or bucket holding secrets that roles granted "read everything" can read.
- **Defaults**: debug or verbose modes on, CORS or allowed hosts open to all, TLS verification off, permissive file modes, a timeout or a size limit removed.
- **Crypto**: a hand-made scheme, a fast hash for passwords or keys, a token compared with `==` instead of in constant time, `random` where `secrets` is meant.
- **Dependencies**: a new dependency: is it needed, maintained, pinned, from the registry the repository uses, and what can it do on install and at run time?

## Evidence

A finding about a secret names where it is and what kind it is, never its value, not even in part: the finding goes to the record and may go to the pull request description.

A finding names the attacker and the gain: "any authenticated caller reads another tenant's keys by changing `tenant_id` in `GET /keys`". A pattern that only looks risky, where you can show the input never comes from outside, is no finding.

## Not here

Bugs without an attacker (Correctness), dependencies the diff does not add or change.
