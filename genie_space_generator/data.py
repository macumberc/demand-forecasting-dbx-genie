"""Deterministic SQL builders driven by a DomainSpec."""

from __future__ import annotations

from typing import Any


def build_table_sqls_from_spec(
    domain_spec: Any,
    fqn: str,
    seed: int,
    scale: int = 1,
) -> dict[str, str]:
    """Build all deterministic CTAS statements from a DomainSpec."""

    sqls: dict[str, str] = {}
    for table in domain_spec.tables:
        sqls[table.table_name] = _build_table_sql(table, fqn, seed, scale)
    return sqls


def build_metric_view_sqls_from_spec(
    domain_spec: Any,
    fqn: str,
) -> dict[str, str]:
    """Build all CREATE VIEW WITH METRICS statements from a DomainSpec."""

    sqls: dict[str, str] = {}
    for mv in domain_spec.metric_views:
        sqls[mv.view_name] = _build_metric_view_sql(mv, fqn)
    return sqls


def get_table_column_comments(domain_spec: Any) -> dict[str, dict[str, str]]:
    """Extract column comments from a DomainSpec for ALTER TABLE statements."""

    comments: dict[str, dict[str, str]] = {}
    for table in domain_spec.tables:
        cols: dict[str, str] = {}
        for col in table.columns:
            if col.comment:
                cols[col.name] = col.comment
        comments[table.table_name] = cols
    return comments


def table_fqdns(domain_spec: Any, fqn: str) -> dict[str, str]:
    """Return fully qualified names for all managed tables."""

    return {t.table_name: f"{fqn}.{t.table_name}" for t in domain_spec.tables}


def metric_view_fqdns(domain_spec: Any, fqn: str) -> dict[str, str]:
    """Return fully qualified names for all metric views."""

    return {mv.view_name: f"{fqn}.{mv.view_name}" for mv in domain_spec.metric_views}


# ---------------------------------------------------------------------------
# Internal SQL builders
# ---------------------------------------------------------------------------


def _start_year(scale: int) -> int:
    """Compute the start year based on scale (scale=1 -> 2025 only)."""
    return 2025 - (scale - 1)


def _build_table_sql(table: Any, fqn: str, seed: int, scale: int) -> str:
    """Build a deterministic CTAS for a single table from its TableSpec."""

    start_year = _start_year(scale)
    archetype = table.entity_dimension  # "transaction", "snapshot", or "forecast"

    # Build VALUES blocks for dimension data
    entities = table.dimension_values
    if not entities:
        raise ValueError(f"Table {table.table_name} has no dimension_values")

    # Determine the value columns from the first entity's keys
    entity_keys = list(entities[0].keys())
    entity_rows = [[e[k] for k in entity_keys] for e in entities]
    entity_values = _values_sql(entity_rows)
    entity_aliases = ", ".join(entity_keys)

    # Determine the category key (first key that contains 'category' or 'type',
    # or fall back to the second key)
    category_key = None
    for k in entity_keys:
        if "category" in k.lower() or "type" in k.lower():
            category_key = k
            break
    if not category_key and len(entity_keys) > 1:
        category_key = entity_keys[1]
    if not category_key:
        category_key = entity_keys[0]

    # Build the primary key column reference for the entity
    entity_pk = entity_keys[0]

    # Build seasonal probability CASE expression
    seasonal = table.seasonal_patterns or {}
    category_dist = table.category_distribution or {}

    case_lines = []
    for cat, month_probs in seasonal.items():
        if isinstance(month_probs, dict):
            for months_str, prob in month_probs.items():
                months = _parse_months(months_str)
                if months:
                    month_list = ", ".join(str(m) for m in months)
                    case_lines.append(
                        f"      WHEN {category_key} = '{_sql_escape(cat)}' "
                        f"AND mo IN ({month_list}) THEN {prob}"
                    )
        # Default probability for this category
        default_prob = category_dist.get(cat, 0.03)
        case_lines.append(
            f"      WHEN {category_key} = '{_sql_escape(cat)}' THEN {default_prob}"
        )

    if not case_lines:
        case_lines.append("      ELSE 0.0300")

    case_expr = "    CASE\n" + "\n".join(case_lines) + "\n      ELSE 0.0300\n    END"

    # Hash-based noise columns
    noise_parts = f"d.dt, e.{entity_pk}"
    qty_noise = _hash_fraction(seed, "qty_noise", "d.dt", f"e.{entity_pk}")
    status_noise = _hash_fraction(seed, "status_noise", "d.dt", f"e.{entity_pk}")
    select_noise = _hash_fraction(seed, "select_noise", "d.dt", f"e.{entity_pk}")
    id_seq = _hash_int(seed, "id_seq", "d.dt", f"e.{entity_pk}", modulo=500)

    # Determine date interval based on archetype
    if archetype == "snapshot":
        interval = "INTERVAL 7 DAY"
    elif archetype == "forecast":
        interval = "INTERVAL 1 MONTH"
    else:
        interval = "INTERVAL 1 DAY"

    # Build column SELECT expressions
    # In the final SELECT, reference the pre-computed column names from skeleton,
    # not the raw hash expressions (which contain d./e. aliases that don't exist here).
    select_cols = []
    for col in table.columns:
        expr = col.generation_expr
        if expr:
            # Replace placeholder tokens with column names from skeleton CTE
            expr = expr.replace("{fqn}", fqn)
            expr = expr.replace("{table}", table.table_name)
            expr = expr.replace("{qty_noise}", "qty_noise")
            expr = expr.replace("{status_noise}", "status_noise")
            expr = expr.replace("{select_noise}", "select_noise")
            expr = expr.replace("{id_seq}", "id_seq")
            expr = expr.replace("{seed}", str(seed))
            # Strip d./e. table aliases — columns are flat in filtered CTE
            expr = _strip_cte_aliases(expr)
            select_cols.append(f"  {expr} AS {col.name}")
        else:
            select_cols.append(f"  {col.name}")

    select_clause = ",\n".join(select_cols)

    # Weekday filter for transaction tables
    weekday_filter = ""
    if archetype == "transaction":
        weekday_filter = "  WHERE DAYOFWEEK(d.dt) BETWEEN 2 AND 6\n"

    # Pair hash filter for snapshot/forecast tables
    pair_filter = ""
    if archetype in ("snapshot", "forecast"):
        pair_hash = _hash_int(seed, "pair_hash", f"e.{entity_pk}", modulo=100)
        pair_filter = f"  WHERE {pair_hash} < 52\n"

    sql = f"""CREATE OR REPLACE TABLE {fqn}.{table.table_name} AS
WITH
entities AS (
  SELECT * FROM VALUES
{entity_values}
  AS t({entity_aliases})
),
date_range AS (
  SELECT EXPLODE(SEQUENCE(DATE'{start_year}-01-01', DATE'2025-12-31', {interval})) AS dt
),
skeleton AS (
  SELECT
    d.dt,
    e.*,
    {qty_noise} AS qty_noise,
    {status_noise} AS status_noise,
    {select_noise} AS select_noise,
    {id_seq} AS id_seq,
    MONTH(d.dt) AS mo
  FROM date_range d
  CROSS JOIN entities e
{weekday_filter}{pair_filter}),
filtered AS (
  SELECT *,
{case_expr} AS selection_prob
  FROM skeleton
)
SELECT
{select_clause}
FROM filtered
WHERE select_noise < selection_prob"""

    return sql.strip()


def _build_metric_view_sql(mv: Any, fqn: str) -> str:
    """Build a CREATE VIEW WITH METRICS statement from a MetricViewSpec."""

    dim_lines = []
    for dim in mv.dimensions:
        dim_lines.append(f"  - name: {dim['name']}")
        dim_lines.append(f"    expr: {dim['expr']}")

    measure_lines = []
    for m in mv.measures:
        measure_lines.append(f"  - name: {m['name']}")
        measure_lines.append(f"    expr: {m['expr']}")
        if m.get("comment"):
            measure_lines.append(f"    comment: {m['comment']}")

    dims_yaml = "\n".join(dim_lines)
    measures_yaml = "\n".join(measure_lines)

    return f"""CREATE OR REPLACE VIEW {fqn}.{mv.view_name}
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
source: {fqn}.{mv.source_table}

dimensions:
{dims_yaml}

measures:
{measures_yaml}
$$""".strip()


# ---------------------------------------------------------------------------
# SQL helpers (replicated from reference)
# ---------------------------------------------------------------------------


def _strip_cte_aliases(expr: str) -> str:
    """Remove d. and e. table alias prefixes from a SQL expression.

    In the skeleton CTE, columns come from ``date_range d`` and ``entities e``.
    By the time the final SELECT runs over ``filtered``, all columns are flat
    so references like ``d.dt`` or ``e.patient_id`` must become ``dt`` / ``patient_id``.

    This version is quote-aware: it only strips aliases outside of single-quoted
    SQL string literals, so patterns like ``'d.value'`` are preserved.
    """
    import re

    # Split on single-quoted segments (including escaped quotes '')
    parts = re.split(r"('(?:''|[^'])*')", expr)
    for i, part in enumerate(parts):
        if i % 2 == 0:  # Not inside quotes
            parts[i] = re.sub(r'\b([de])\.(\w+)', r'\2', part)
    return "".join(parts)


def _values_sql(rows: list[list[object]]) -> str:
    """Render rows into a deterministic SQL VALUES block."""

    lines = []
    for idx, row in enumerate(rows):
        prefix = "    " if idx == 0 else "  , "
        rendered = ", ".join(_sql_value(value) for value in row)
        lines.append(f"{prefix}({rendered})")
    return "\n".join(lines)


def _sql_value(value: object) -> str:
    """Render a scalar as a SQL literal."""

    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    return str(value)


def _sql_escape(value: str) -> str:
    """Escape single quotes in a SQL string."""
    return value.replace("'", "''")


def _hash_fraction(seed: int, salt: str, *parts: str, scale: int = 10000) -> str:
    """Create a deterministic pseudo-random decimal in the range [0, 1)."""

    sql_parts = ", ".join([f"'{seed}'", f"'{salt}'", *parts])
    return f"(CAST(pmod(hash({sql_parts}), {scale}) AS DOUBLE) / {scale}.0)"


def _hash_int(
    seed: int,
    salt: str,
    *parts: str,
    modulo: int,
    offset: int = 0,
) -> str:
    """Create a deterministic pseudo-random integer."""

    sql_parts = ", ".join([f"'{seed}'", f"'{salt}'", *parts])
    base = f"pmod(hash({sql_parts}), {modulo})"
    if offset == 0:
        return base
    if offset > 0:
        return f"({base} + {offset})"
    return f"({base} - {abs(offset)})"


def _parse_months(months_str: str) -> list[int]:
    """Parse a months string like '11,12' or '1-3' into a list of ints."""

    result = []
    for part in str(months_str).split(","):
        part = part.strip()
        if "-" in part:
            start, end = part.split("-", 1)
            try:
                result.extend(range(int(start), int(end) + 1))
            except ValueError:
                pass
        else:
            try:
                result.append(int(part))
            except ValueError:
                pass
    return result
