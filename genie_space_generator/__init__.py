"""Genie Space Generator — deploy a Genie data room for any industry and use case."""

from __future__ import annotations

from typing import Any, Optional

from .cleanup import cleanup as _cleanup
from .config import DEFAULT_SEED
from .data import (
    build_metric_view_sqls_from_spec,
    build_table_sqls_from_spec,
    get_table_column_comments,
    metric_view_fqdns,
    table_fqdns,
)
from .genie import create_or_replace_genie_space, resolve_warehouse_id
from .generator import DomainSpec, generate_domain_spec
from .results import DeploymentResult, GenieSpaceResult
from .validators import catalog_exists, current_catalog, resolve_namespace, sql_string

try:
    from importlib.metadata import version as _version

    __version__ = _version("genie-space-generator")
except Exception:
    __version__ = "0.0.0-dev"


_PREFIX = "[genie-space-generator]"

_CATALOG_FALLBACK_ERRORS = ("PERMISSION_DENIED", "UNAUTHORIZED", "INVALID_STATE")


def _log(msg: str) -> None:
    print(f"{_PREFIX} {msg}")


def _display_html(html: str) -> None:
    try:
        import IPython  # type: ignore[import-untyped]

        ip = IPython.get_ipython()
        if ip and hasattr(ip, "user_ns") and "displayHTML" in ip.user_ns:
            ip.user_ns["displayHTML"](html)
    except Exception:
        pass


_SUMMARY_HTML = """\
<div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            max-width: 680px; margin: 16px 0;">
  <div style="background: #f8f9fa; border: 1px solid #dee2e6; border-radius: 8px;
              padding: 24px; margin-bottom: 16px;">
    <h2 style="margin: 0 0 16px 0; color: #1b3a4b; font-size: 20px;">
      %(title)s &mdash; Setup Complete
    </h2>
    <table style="width: 100%%; border-collapse: collapse; font-size: 14px;">
      <tr style="border-bottom: 1px solid #dee2e6;">
        <td style="padding: 6px 0; color: #6c757d;">Schema</td>
        <td style="padding: 6px 0; font-weight: 600; font-family: monospace;">%(fqn)s</td>
      </tr>
      %(table_rows)s
      %(metric_view_row)s
      <tr style="border-top: 2px solid #adb5bd;">
        <td style="padding: 6px 0; color: #6c757d; font-weight: 600;">Total</td>
        <td style="padding: 6px 0; font-weight: 600;">%(total)s rows</td>
      </tr>
    </table>
  </div>
  %(genie_button)s
  <div style="background: #fff3cd; border: 1px solid #ffc107; border-radius: 8px;
              padding: 16px; margin-top: 12px;">
    <div style="font-size: 13px; color: #664d03; margin-bottom: 8px; font-weight: 600;">
      Cleanup &mdash; run this to remove everything:
    </div>
    <pre style="background: #1e1e1e; color: #d4d4d4; padding: 12px; border-radius: 6px;
                font-size: 13px; margin: 0; overflow-x: auto;"><code>from genie_space_generator import teardown
teardown(spark, **result)</code></pre>
  </div>
</div>
"""

_GENIE_BUTTON_HTML = """\
<div style="margin-bottom: 4px;">
  <a href="%(genie_url)s" target="_blank"
     style="display: inline-block; background: #1b3a4b; color: white; padding: 12px 28px;
            border-radius: 6px; text-decoration: none; font-size: 15px; font-weight: 600;
            letter-spacing: 0.3px;">
    Open Genie Space
  </a>
</div>
"""


def deploy(
    spark: Any,
    industry: str,
    company_name: str,
    use_case: str,
    business_context: str,
    num_tables: int = 3,
    num_products: int = 20,
    num_locations: int = 8,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
    warehouse_id: Optional[str] = "auto",
    seed: int = DEFAULT_SEED,
    scale: int = 1,
) -> dict[str, Any]:
    """Deploy a custom Genie data room for any industry and use case.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session (available as ``spark`` in Databricks notebooks).
    industry : str
        Industry vertical (e.g., "Healthcare", "Retail", "Manufacturing").
    company_name : str
        Fictional company name for the demo scenario.
    use_case : str
        Short description of the analytics use case.
    business_context : str
        2-3 sentence description of the business scenario.
    num_tables : int
        Number of tables to generate (default 3).
    num_products : int
        Number of entity items per dimension (default 20).
    num_locations : int
        Number of locations/facilities/sites (default 8).
    catalog : str, optional
        Target catalog. Defaults to the workspace's current catalog.
    schema : str, optional
        Target schema. Defaults to ``{basename}_{username}``.
    warehouse_id : str, optional
        SQL warehouse ID for the Genie space. ``"auto"`` (default)
        auto-detects the best available warehouse. Pass ``None`` to skip
        Genie space creation.
    seed : int
        Deterministic seed for data generation.
    scale : int
        Data size multiplier. ``scale=1`` = 1 year (2025), ``scale=5`` = 5 years.

    Returns
    -------
    dict
        Pass this dict as ``**result`` to :func:`teardown` to remove everything.
    """
    if scale < 1:
        raise ValueError("scale must be >= 1")

    # 1. Generate domain spec via LLM
    _log(f"Generating domain model for {industry} / {company_name} ...")
    domain_spec = generate_domain_spec(
        spark,
        industry=industry,
        company_name=company_name,
        use_case=use_case,
        business_context=business_context,
        num_tables=num_tables,
        num_products=num_products,
        num_locations=num_locations,
    )
    _log(
        f"Domain model ready: {len(domain_spec.tables)} tables, "
        f"{len(domain_spec.metric_views)} metric views, "
        f"{len(domain_spec.sample_questions)} sample questions"
    )

    # 2. Resolve namespace
    ns = resolve_namespace(
        spark,
        catalog=catalog,
        schema=schema,
        schema_basename=domain_spec.schema_basename,
    )

    _log(f"Catalog  : {ns.catalog}")
    _log(f"Schema   : {ns.fqn}")

    # 3. Create catalog with permission-denied fallback
    catalog_attempted = False
    if catalog_exists(spark, ns.catalog):
        _log(f"Catalog '{ns.catalog}' already exists — skipping creation")
    else:
        try:
            spark.sql(f"CREATE CATALOG IF NOT EXISTS {ns.catalog}")
            catalog_attempted = True
        except Exception as exc:
            msg = str(exc)
            if any(err in msg for err in _CATALOG_FALLBACK_ERRORS):
                fallback = current_catalog(spark)
                _log(f"Cannot create catalog '{ns.catalog}' — falling back to '{fallback}'")
                ns = resolve_namespace(
                    spark,
                    catalog=fallback,
                    schema=ns.schema,
                    schema_basename=domain_spec.schema_basename,
                )
                _log(f"Catalog  : {ns.catalog}")
                _log(f"Schema   : {ns.fqn}")
            else:
                raise

    # 4. Create schema with permission-denied fallback
    schema_comment = f"{domain_spec.company_name} {domain_spec.use_case} demo data"
    try:
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {ns.fqn} "
            f"COMMENT '{sql_string(schema_comment)}'"
        )
    except Exception as exc:
        msg = str(exc)
        if any(err in msg for err in _CATALOG_FALLBACK_ERRORS):
            fallback = current_catalog(spark)
            if ns.catalog != fallback:
                _log(f"Cannot create schema in '{ns.catalog}' — falling back to '{fallback}'")
                ns = resolve_namespace(
                    spark,
                    catalog=fallback,
                    schema=ns.schema,
                    schema_basename=domain_spec.schema_basename,
                )
                _log(f"Catalog  : {ns.catalog}")
                _log(f"Schema   : {ns.fqn}")
                spark.sql(
                    f"CREATE SCHEMA IF NOT EXISTS {ns.fqn} "
                    f"COMMENT '{sql_string(schema_comment)}'"
                )
            else:
                raise
        else:
            raise
    _log(f"Schema ready: {ns.fqn}")

    # 5. Build and execute CTAS statements
    sqls = build_table_sqls_from_spec(domain_spec, ns.fqn, seed, scale)
    tables: dict[str, int] = {}
    for name, sql in sqls.items():
        _log(f"Creating {name} ...")
        spark.sql(sql)
        cnt = spark.table(f"{ns.fqn}.{name}").count()
        tables[name] = cnt
        _log(f"  {name}: {cnt:,} rows")

    # 6. Apply column comments
    _log("Adding column comments ...")
    column_comments = get_table_column_comments(domain_spec)
    for table_name, cols in column_comments.items():
        for col, comment in cols.items():
            spark.sql(
                f"ALTER TABLE {ns.fqn}.{table_name} "
                f"ALTER COLUMN {col} COMMENT '{sql_string(comment)}'"
            )
    _log("  Column comments applied")

    # 7. Create metric views
    _log("Creating metric views ...")
    mv_sqls = build_metric_view_sqls_from_spec(domain_spec, ns.fqn)
    for mv_name, mv_sql in mv_sqls.items():
        _log(f"  {mv_name}")
        spark.sql(mv_sql)
    _log(f"  {len(mv_sqls)} metric views created")

    # 8. Auto-detect warehouse
    resolved_wh, skip_reason = resolve_warehouse_id(spark, warehouse_id)
    if skip_reason:
        _log(f"Genie: {skip_reason}")

    # 9. Create/replace Genie space
    genie: GenieSpaceResult
    if resolved_wh:
        _log(f"Warehouse: {resolved_wh}")
        try:
            genie = create_or_replace_genie_space(
                spark, domain_spec, ns.fqn, resolved_wh, ns.username
            )
            _log(f"Genie space {genie.status}: {genie.url}")
        except Exception as exc:
            _log(f"WARNING: Genie space creation failed: {exc}")
            genie = GenieSpaceResult(
                status="failed", requested=True, reason=str(exc)
            )
    else:
        genie = GenieSpaceResult(
            status="skipped", requested=False, reason=skip_reason
        )

    result = DeploymentResult(
        catalog=ns.catalog,
        schema=ns.schema,
        fqn=ns.fqn,
        seed=seed,
        schema_created=True,
        catalog_attempted=catalog_attempted,
        tables=tables,
        table_fqdns=table_fqdns(domain_spec, ns.fqn),
        metric_view_fqdns=metric_view_fqdns(domain_spec, ns.fqn),
        warehouse_id=resolved_wh,
        genie=genie,
    )

    # 10. Display summary
    print()
    _log("=" * 50)
    _log("SETUP COMPLETE")
    _log("=" * 50)
    total = sum(tables.values())
    for t, cnt in tables.items():
        _log(f"  {t:30s}  {cnt:>6,} rows")
    _log(f"  {'TOTAL':30s}  {total:>6,} rows")
    if genie.url:
        _log(f"  Genie: {genie.url}")

    table_rows = "".join(
        f'<tr style="border-bottom: 1px solid #dee2e6;">'
        f'<td style="padding: 6px 0; color: #6c757d;">{t}</td>'
        f'<td style="padding: 6px 0; font-family: monospace;">{cnt:,} rows</td>'
        f"</tr>"
        for t, cnt in tables.items()
    )
    metric_view_row = (
        '<tr style="border-bottom: 1px solid #dee2e6;">'
        '<td style="padding: 6px 0; color: #6c757d;">Metric Views</td>'
        f'<td style="padding: 6px 0; font-family: monospace;">{len(mv_sqls)} views</td>'
        "</tr>"
    )
    genie_button = (
        (_GENIE_BUTTON_HTML % {"genie_url": genie.url}) if genie.url else ""
    )
    _display_html(
        _SUMMARY_HTML
        % {
            "title": domain_spec.company_name,
            "fqn": ns.fqn,
            "table_rows": table_rows,
            "total": f"{total:,}",
            "metric_view_row": metric_view_row,
            "genie_button": genie_button,
        }
    )

    return result.as_dict()


def teardown(spark: Any, **kwargs: Any) -> dict[str, Any]:
    """Remove all resources created by :func:`deploy`.

    The easiest way to call this is to unpack the dict returned by ``deploy``::

        result = deploy(spark, ...)
        teardown(spark, **result)
    """
    return _cleanup(spark, deployment=kwargs)


__all__ = ["deploy", "teardown"]
