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
