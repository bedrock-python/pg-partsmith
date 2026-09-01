# pg-partsmith

**PostgreSQL partition lifecycle management with a plan you can read before it runs.**

pg-partsmith keeps the partitions of a PostgreSQL table in the shape you describe. You say
what the tree should look like — monthly partitions, twelve of them kept, three created
ahead, each split by tenant — and it reads what actually exists, works out the difference,
shows you the plan, and applies it. No extension, no superuser, no scheduler of its own.

!!! agents "Building this with an AI assistant?"

    Hand it **[one page](agents.md)** instead of this site. It carries the whole API
    surface, the rules that break code when they are broken, the mistakes models actually
    make, and a map of which page to fetch for everything it leaves out. Every page here
    is also served as raw Markdown at its own URL — `/agents.md`,
    `/guide/foreign-keys.md` — and the **Copy page** button at the top of each one hands
    it straight to a chat window.

```python
config = TablePartitionConfig(
    schema="public",
    table_name="events",
    partition_column="created_at",
    granularity=PartitionGranularity.MONTH,
    create_ahead_count=3,
    retention_count=12,
)

plan = await service.plan(config)      # reads the catalog, issues no DDL
print(plan.describe())
```

```text
plan for public.events at 2026-08-28T10:00:00+00:00
  CREATE public.events__2026_08 (create_ahead)
  CREATE public.events__2026_09 (create_ahead)
  CREATE public.events__2026_10 (create_ahead)
```

```python
result = await service.apply(config, plan)   # takes the table's lock, runs the DDL
```

## Why you might want it

- **You can see what will happen before it happens.** Every run is a plan first: typed
  operations, each with the reason it is there, and the things the planner decided *not*
  to touch, each with the reason for that.
- **It never drops what it does not own.** A partition someone attached by hand, a foreign
  table, a table with an unreadable bound — inspected, reported, left alone. Destructive
  operations are re-checked against the catalog at the moment they run.
- **It covers the shapes real systems use.** Monthly and weekly tables, UUIDv7 and epoch
  keys, id-partitioned queues, hash buckets per tenant under each period, sliding lists
  rotated by application state, foreign tables as cold storage — the shapes found in
  GlitchTip, GitLab, PGMQ, Hatchet, pg_partman and pg_clickhouse.
- **It fits your stack.** Async (`AsyncEngine`) or sync (`Engine`) SQLAlchemy, any
  scheduler, PostgreSQL advisory or Redis locks, hooks for archiving, Pydantic
  configuration that round-trips through JSON and environment variables.

## Where to start

<div class="grid cards" markdown>

- **Getting started**

    Install, partition your first table, see a plan, run a tick, put it on a schedule.

    [Start here →](getting-started/installation.md)

- **Concepts**

    How the library thinks: schemes, boundaries, lifecycle policies, the plan, ownership.

    [How it works →](concepts/overview.md)

- **How-to guides**

    Backfill history, migrate from pg_partman, archive before dropping, handle foreign keys.

    [Guides →](guide/configuration.md)

- **Reference**

    Every class, every configuration field, every finding and error message.

    [API reference →](reference/index.md)

- **For AI agents**

    One page holding the whole API surface, the rules that break code when broken,
    and a map of everything else — to hand to a coding assistant instead of the site.

    [Agent context →](agents.md)

</div>

## At a glance

| | |
|---|---|
| Requirements | Python 3.11+, PostgreSQL 15+ (tested on 15, 16 and 17), SQLAlchemy 2 |
| Install | `pip install pg-partsmith` |
| Partitioning | `RANGE` over time, encoded keys (UUIDv7, epoch) and integers; `LIST` (static and sliding); `HASH`; nested to any depth |
| Lifecycle | create ahead / until a horizon / when the newest partition is full; keep by count, age, distance or predicate; detach, then drop after a grace period |
| Safety | plan before apply; ownership by catalog, never by name; OID revalidation; no `CASCADE`, ever |
| Source | [github.com/bedrock-python/pg-partsmith](https://github.com/bedrock-python/pg-partsmith) |
