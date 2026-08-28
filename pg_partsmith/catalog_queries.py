"""Shared pg_catalog SQL used by both the repositories and the metadata providers.

Plain-string constants (bind names ``:table_name`` / ``:partition_name`` and
friends) so the aio and sync mirrors wrap one canonical query text instead of
hand-copying it.
"""

# A relation that can be a partition: a table, a partitioned table, or a
# foreign table.
RELATION_EXISTS_SQL = """
    SELECT EXISTS (
        SELECT 1
        FROM pg_class
        WHERE oid = to_regclass(:partition_name)
          AND relkind IN ('r', 'p', 'f')
    )
"""

RELATION_KIND_SQL = """
    SELECT c.relkind
    FROM pg_class c
    WHERE c.oid = to_regclass(:name)
"""

RELATION_OID_SQL = """
    SELECT c.oid
    FROM pg_class c
    WHERE c.oid = to_regclass(:name)
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
# The hierarchy is walked over ``pg_inherits`` rather than with
# ``pg_partition_tree``: once the first transaction of a ``DETACH CONCURRENTLY``
# has committed, the function omits the half-detached partition -- the same
# rule that hides it from queries through the parent -- and the planner would
# never learn that it needs finalizing. A recursive walk over the catalog sees
# it, with ``inhdetachpending`` set. The root is a partitioned table or a
# partition, as for ``pg_partition_tree``; a plain table yields no rows.
#
# Each row carries both halves of a node's identity: ``relpartbound`` says how
# it sits inside its parent, while ``partstrat``/``partattrs`` say how it
# partitions its own children -- a branch has both, a leaf only the first.
# ``relkind`` tells a foreign leaf from a local one, and ``oid`` is what a
# destructive operation is revalidated against.
PARTITION_TREE_SQL = """
    WITH RECURSIVE t AS (
        SELECT root.oid AS relid, CAST(NULL AS oid) AS parentrelid, 0 AS level
        FROM pg_class root
        WHERE root.oid = to_regclass(:table_name)
          AND (root.relkind = 'p' OR root.relispartition)
        UNION ALL
        SELECT child.inhrelid, child.inhparent, t.level + 1
        FROM pg_inherits child
        JOIN t ON child.inhparent = t.relid
    )
    SELECT
        t.level AS level,
        cl.oid AS oid,
        cl.relkind AS relkind,
        ns.nspname AS partition_schema,
        cl.relname AS partition_name,
        pns.nspname AS parent_schema,
        p.relname AS parent_name,
        pg_get_expr(cl.relpartbound, cl.oid) AS boundaries,
        cl.relispartition AS is_attached,
        COALESCE(inh.inhdetachpending, false) AS detach_pending,
        pt.partstrat AS partstrat,
        (
            SELECT array_agg(a.attname ORDER BY k.ord)
            FROM unnest(pt.partattrs) WITH ORDINALITY AS k(attnum, ord)
            LEFT JOIN pg_attribute a ON a.attrelid = pt.partrelid AND a.attnum = k.attnum
        ) AS partition_columns,
        -- A key position holding an expression is recorded as attnum 0, which
        -- matches no column. Inner-joining it away would report a shorter key
        -- than the relation has, and a shortened key compares *equal* to a
        -- one-column spec -- so the mismatch guard would never fire.
        pt.partnatts AS key_arity
    FROM t
    JOIN pg_class cl ON cl.oid = t.relid
    JOIN pg_namespace ns ON ns.oid = cl.relnamespace
    LEFT JOIN pg_class p ON p.oid = t.parentrelid
    LEFT JOIN pg_namespace pns ON pns.oid = p.relnamespace
    LEFT JOIN pg_inherits inh ON inh.inhrelid = cl.oid AND inh.inhparent = t.parentrelid
    LEFT JOIN pg_partitioned_table pt ON pt.partrelid = cl.oid
    ORDER BY t.level, ns.nspname, cl.relname
"""

# Marker-tagged detached tables whose marker names one of ``:markers``.
#
# The first comment line is the ownership marker; the whole comment is
# returned so the detach instant on the second line can be read too.
ORPHANS_SQL = """
    SELECT
        c.oid AS oid,
        c.relkind AS relkind,
        ns.nspname AS partition_schema,
        c.relname AS partition_name,
        d.description AS description
    FROM pg_class c
    JOIN pg_namespace ns ON c.relnamespace = ns.oid
    JOIN pg_description d
      ON d.objoid = c.oid
     AND d.classoid = 'pg_class'::regclass
     AND d.objsubid = 0
    WHERE c.relkind IN ('r', 'p', 'f')
      AND c.relispartition = false
      AND split_part(d.description, E'\\n', 1) = ANY(CAST(:markers AS text[]))
      AND NOT EXISTS (
          SELECT 1
          FROM pg_inherits inh
          WHERE inh.inhrelid = c.oid
      )
    ORDER BY ns.nspname, c.relname
"""

# Size and row estimate of each relation in ``:oids``, subtree included.
#
# ``pg_total_relation_size`` of a partitioned relation is 0 -- it has no
# storage of its own -- so sizes are summed over the leaves of each subtree.
# ``pg_partition_tree`` returns nothing for a relation that is neither
# partitioned nor a partition -- a detached leaf, exactly the kind an orphan
# usually is -- so such a relation is measured as its own single leaf.
# Rows come from the statistics collector, never from ``COUNT(*)``: a plan
# must not scan a 500 GB partition to decide what to do with it.
PARTITION_FACTS_SQL = """
    SELECT
        root.oid AS oid,
        COALESCE(SUM(pg_total_relation_size(t.relid)), 0) AS size_bytes,
        COALESCE(SUM(COALESCE(s.n_live_tup, 0)), 0) AS row_estimate
    FROM unnest(CAST(:oids AS oid[])) AS root(oid)
    CROSS JOIN LATERAL (
        SELECT pt.relid
        FROM pg_partition_tree(root.oid) pt
        WHERE pt.isleaf
        UNION ALL
        SELECT root.oid
        WHERE NOT EXISTS (SELECT 1 FROM pg_partition_tree(root.oid))
    ) t
    LEFT JOIN pg_stat_user_tables s ON s.relid = t.relid
    GROUP BY root.oid
"""

# Foreign keys pointing at any of ``:oids`` -- a partitioned parent or a
# partition itself -- with the referencing relation spelled ready to splice
# into SQL and both column lists in constraint order.
#
# Only top-level constraints (``conparentid = 0``): the clone PostgreSQL keeps
# on every partition for a foreign key that references the parent has the same
# referencing side, and would be counted twice.
INCOMING_FOREIGN_KEYS_SQL = """
    SELECT
        con.conname AS constraint_name,
        con.confrelid AS referenced_oid,
        quote_ident(ns.nspname) || '.' || quote_ident(c.relname) AS referencing,
        (
            SELECT array_agg(a.attname ORDER BY k.ord)
            FROM unnest(con.conkey) WITH ORDINALITY AS k(attnum, ord)
            JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = k.attnum
        ) AS referencing_columns,
        (
            SELECT array_agg(a.attname ORDER BY k.ord)
            FROM unnest(con.confkey) WITH ORDINALITY AS k(attnum, ord)
            JOIN pg_attribute a ON a.attrelid = con.confrelid AND a.attnum = k.attnum
        ) AS referenced_columns
    FROM pg_constraint con
    JOIN pg_class c ON c.oid = con.conrelid
    JOIN pg_namespace ns ON ns.oid = c.relnamespace
    WHERE con.contype = 'f'
      AND con.conparentid = 0
      AND con.confrelid = ANY(CAST(:oids AS oid[]))
    ORDER BY con.conname
"""

# A relation's live column names, in its own physical order.
#
# Two partitions of the same table always share column *names*, but not
# necessarily their order: ATTACH PARTITION matches by name, so a partition
# created independently and attached can sit in a different physical order than
# one created with LIKE. Anything copying rows between them has to name the
# columns rather than rely on position.
RELATION_COLUMNS_SQL = """
    SELECT a.attname
    FROM pg_attribute a
    WHERE a.attrelid = to_regclass(:table_name)
      AND a.attnum > 0
      AND NOT a.attisdropped
    ORDER BY a.attnum
"""

# A relation's live columns with their types, for a foreign table shaped like it.
#
# ``format_type`` renders the type the way ``CREATE TABLE`` accepts it,
# typmod included (``numeric(10,2)``, ``character varying(80)``); names of
# custom types come back quoted where they need to be.
RELATION_COLUMN_DEFINITIONS_SQL = """
    SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS type_name, a.attnotnull
    FROM pg_attribute a
    WHERE a.attrelid = to_regclass(:table_name)
      AND a.attnum > 0
      AND NOT a.attisdropped
    ORDER BY a.attnum
"""

# A relation's owner and every grant on it, one row per grant (one row with
# no grant when the ACL is empty, which is how a fresh table looks).
#
# ``aclexplode`` reports PUBLIC as grantee 0, which ``regrole`` cannot name.
RELATION_PRIVILEGES_SQL = """
    SELECT
        pg_get_userbyid(c.relowner) AS owner,
        CASE WHEN acl.grantee = 0 THEN 'PUBLIC' ELSE acl.grantee::regrole::text END AS grantee,
        acl.privilege_type AS privilege_type,
        acl.is_grantable AS is_grantable
    FROM pg_class c
    LEFT JOIN LATERAL aclexplode(c.relacl) acl ON true
    WHERE c.oid = to_regclass(:table_name)
    ORDER BY grantee, privilege_type
"""

# Columns of every uniqueness-enforcing structure on a relation.
#
# PostgreSQL requires each of these on a partitioned table to contain every
# partition-key column, so this is what decides whether a subpartition column is
# usable before any DDL is attempted.
#
# Read from ``pg_index`` rather than ``pg_constraint``, because the rule applies
# to bare ``CREATE UNIQUE INDEX`` as much as to a named UNIQUE/PRIMARY KEY --
# and ``LIKE ... INCLUDING ALL`` copies the index either way, so a branch built
# over one is refused just the same. EXCLUDE constraints carry the same
# requirement but their backing index is not marked unique, so they are unioned
# in separately.
#
# Only the *key* columns are read: a covering index's INCLUDE columns do not
# satisfy the requirement, and counting them would let a config through that
# PostgreSQL then rejects.
UNIQUE_CONSTRAINT_COLUMNS_SQL = """
    SELECT constraint_name, columns
    FROM (
        SELECT
            ic.relname AS constraint_name,
            (
                SELECT array_agg(a.attname ORDER BY k.ord)
                FROM unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord)
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
                WHERE k.ord <= i.indnkeyatts
            ) AS columns
        FROM pg_index i
        JOIN pg_class ic ON ic.oid = i.indexrelid
        WHERE i.indrelid = to_regclass(:table_name)
          AND i.indisunique

        UNION ALL

        SELECT
            c.conname AS constraint_name,
            (
                SELECT array_agg(a.attname ORDER BY k.ord)
                FROM unnest(c.conkey) WITH ORDINALITY AS k(attnum, ord)
                JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k.attnum
            ) AS columns
        FROM pg_constraint c
        WHERE c.conrelid = to_regclass(:table_name)
          AND c.contype = 'x'
    ) AS enforcing
    WHERE columns IS NOT NULL
    ORDER BY constraint_name
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

INSTANT_HAS_PASSED_SQL = """
    SELECT now() >= CAST(:upper_bound AS timestamptz) + make_interval(secs => :settle_seconds)
"""

# The same comparison for a bound still in its catalog text form.
#
# The cast happens in SQL so the session's timezone decides how a naive literal
# is read -- the same rule ATTACH used to write it. Casting the parameter to
# text first keeps the driver from binding it as a timestamp and rejecting it
# before PostgreSQL ever sees it.
TEXT_INSTANT_HAS_PASSED_SQL = """
    SELECT now() >= CAST(:upper_bound AS text)::timestamptz + make_interval(secs => :settle_seconds)
"""

# The table's own partition key, in key order.
#
# ``partattrs`` is an ordered vector, and its order is the key order -- which is
# not the column order. Unnesting WITH ORDINALITY preserves it; ordering by
# attnum would silently transpose a composite key.
# A key position holding an expression rather than a column is recorded as
# attnum 0, which matches no pg_attribute row. The join is LEFT so that position
# still comes back -- as NULL -- because dropping it would silently shorten the
# key and every bound built from it would be the wrong arity.
PARTITION_COLUMNS_SQL = """
    SELECT a.attname
    FROM pg_partitioned_table t
    CROSS JOIN LATERAL unnest(t.partattrs) WITH ORDINALITY AS k(attnum, ord)
    LEFT JOIN pg_attribute a ON a.attrelid = t.partrelid AND a.attnum = k.attnum
    WHERE t.partrelid = to_regclass(:table_name)
    ORDER BY k.ord
"""

# Last value handed out by the serial/identity sequence feeding a column.
#
# NULL when the column has no sequence, or the sequence was never used.
SEQUENCE_LAST_VALUE_SQL = """
    SELECT pg_sequence_last_value(CAST(pg_get_serial_sequence(:table_name, :column) AS regclass))
"""

# A relation's own partitioning, for one name.
PARTITION_STRATEGY_SQL = """
    SELECT partstrat
    FROM pg_partitioned_table t
    WHERE t.partrelid = to_regclass(:table_name)
"""
