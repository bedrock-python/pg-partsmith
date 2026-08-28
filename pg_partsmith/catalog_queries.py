"""Shared pg_catalog SQL used by both the resolvers and the metadata providers.

Plain-string constants (bind names ``:table_name`` / ``:partition_name``) so
the aio and sync mirrors wrap one canonical query text instead of hand-copying
it.
"""

RELATION_EXISTS_SQL = """
    SELECT EXISTS (
        SELECT 1
        FROM pg_class
        WHERE oid = to_regclass(:partition_name)
          AND relkind IN ('r', 'p')
    )
"""

PARTITION_IS_ATTACHED_SQL = """
    SELECT EXISTS (
        SELECT 1
        FROM pg_inherits inh
        JOIN pg_class child ON inh.inhrelid = child.oid
        WHERE inh.inhparent = to_regclass(:table_name)
          AND inh.inhrelid = to_regclass(:partition_name)
          AND child.relispartition = true
    )
"""

# Whole partition tree below one relation, in a single round-trip.
#
# ``pg_partition_tree`` walks the hierarchy for us, so nested trees cost one
# query no matter how deep they go. Each row carries both halves of a node's
# identity: ``relpartbound`` says how it sits inside its parent, while
# ``partstrat``/``partkeydef`` say how it partitions its own children — a
# branch has both, a leaf only the first.
PARTITION_TREE_SQL = """
    SELECT
        t.level AS level,
        ns.nspname AS partition_schema,
        cl.relname AS partition_name,
        pns.nspname AS parent_schema,
        p.relname AS parent_name,
        pg_get_expr(cl.relpartbound, cl.oid) AS boundaries,
        cl.relispartition AS is_attached,
        pt.partstrat AS partstrat,
        (
            SELECT array_agg(a.attname ORDER BY k.ord)
            FROM unnest(pt.partattrs) WITH ORDINALITY AS k(attnum, ord)
            JOIN pg_attribute a ON a.attrelid = pt.partrelid AND a.attnum = k.attnum
        ) AS partition_columns
    FROM pg_partition_tree(to_regclass(:table_name)) t
    JOIN pg_class cl ON cl.oid = t.relid
    JOIN pg_namespace ns ON ns.oid = cl.relnamespace
    LEFT JOIN pg_class p ON p.oid = t.parentrelid
    LEFT JOIN pg_namespace pns ON pns.oid = p.relnamespace
    LEFT JOIN pg_partitioned_table pt ON pt.partrelid = cl.oid
    WHERE t.relid IS NOT NULL
    ORDER BY t.level, ns.nspname, cl.relname
"""

# Columns of every UNIQUE / PRIMARY KEY constraint on a relation.
#
# PostgreSQL requires such a constraint on a partitioned table to contain every
# partition-key column, so this is what decides whether a subpartition column is
# usable before any DDL is attempted.
UNIQUE_CONSTRAINT_COLUMNS_SQL = """
    SELECT
        c.conname AS constraint_name,
        (
            SELECT array_agg(a.attname ORDER BY a.attnum)
            FROM unnest(c.conkey) AS k(attnum)
            JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k.attnum
        ) AS columns
    FROM pg_constraint c
    WHERE c.conrelid = to_regclass(:table_name)
      AND c.contype IN ('p', 'u')
    ORDER BY c.conname
"""

# Upper bound of an attached RANGE partition, as raw catalog text.
#
# The cast to timestamptz is guarded by a shape check rather than applied
# blindly: a partition keyed by an encoded identifier has a perfectly valid
# upper bound that is not a timestamp, and casting it would raise instead of
# reporting "not closed".
PARTITION_UPPER_BOUND_SQL = r"""
    SELECT (regexp_match(
                pg_get_expr(c.relpartbound, c.oid),
                'TO \(''([^'']+)'''
           ))[1] AS upper_bound
    FROM pg_class c
    JOIN pg_inherits i ON i.inhrelid = c.oid
    JOIN pg_partitioned_table pt ON pt.partrelid = i.inhparent
    WHERE c.oid = to_regclass(:partition_name)
      AND pt.partstrat = 'r'
"""

PARTITION_CLOSED_SQL = r"""
    SELECT now() >= b.upper_bound + make_interval(secs => :settle_seconds)
    FROM (
        SELECT CASE
                   WHEN raw ~ '^\d{4}-\d{2}-\d{2}' THEN raw::timestamptz
               END AS upper_bound
        FROM (
            SELECT (regexp_match(
                        pg_get_expr(c.relpartbound, c.oid),
                        'TO \(''([^'']+)'''
                   ))[1] AS raw
            FROM pg_class c
            JOIN pg_inherits i ON i.inhrelid = c.oid
            JOIN pg_partitioned_table pt ON pt.partrelid = i.inhparent
            WHERE c.oid = to_regclass(:partition_name)
              AND pt.partstrat = 'r'
        ) AS parsed
    ) AS b
    WHERE b.upper_bound IS NOT NULL
"""

INSTANT_HAS_PASSED_SQL = """
    SELECT now() >= CAST(:upper_bound AS timestamptz) + make_interval(secs => :settle_seconds)
"""
