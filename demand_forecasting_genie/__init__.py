"""Demand Forecasting & Inventory Management — Genie Data Room for Databricks."""

from __future__ import annotations

from typing import Any, Optional

from .cleanup import cleanup as _cleanup
from .config import DEFAULT_SCHEMA_COMMENT, DEFAULT_SEED
from .data import TABLE_COLUMN_COMMENTS, build_table_sqls, table_fqdns
from .genie import create_or_replace_genie_space, resolve_warehouse_id
from .results import DeploymentResult, GenieSpaceResult
from .validators import current_catalog, resolve_namespace, sql_string

try:
    from importlib.metadata import version as _version

    __version__ = _version("demand-forecasting-genie")
except Exception:
    __version__ = "0.0.0-dev"


_PREFIX = "[demand-forecasting-genie]"


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
      NorthStar Logistics &mdash; Setup Complete
    </h2>
    <table style="width: 100%%; border-collapse: collapse; font-size: 14px;">
      <tr style="border-bottom: 1px solid #dee2e6;">
        <td style="padding: 6px 0; color: #6c757d;">Schema</td>
        <td style="padding: 6px 0; font-weight: 600; font-family: monospace;">%(fqn)s</td>
      </tr>
      %(table_rows)s
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
                font-size: 13px; margin: 0; overflow-x: auto;"><code>from demand_forecasting_genie import teardown
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
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
    warehouse_id: Optional[str] = "auto",
    seed: int = DEFAULT_SEED,
) -> dict[str, Any]:
    """Deploy the NorthStar Logistics demand-forecasting Genie data room.

    Creates a Unity Catalog schema, generates three deterministic synthetic
    tables, and provisions a fully-configured Genie space.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session (available as ``spark`` in Databricks notebooks).
    catalog : str, optional
        Target catalog.  Defaults to the workspace's current catalog.
    schema : str, optional
        Target schema.  Defaults to ``demand_forecasting_<username>``.
    warehouse_id : str, optional
        SQL warehouse ID for the Genie space.  ``"auto"`` (default)
        auto-detects the best available warehouse.  Pass ``None`` to skip
        Genie space creation entirely.
    seed : int
        Deterministic seed for data generation.

    Returns
    -------
    dict
        Pass this dict as ``**result`` to :func:`teardown` to remove everything.
    """
    ns = resolve_namespace(spark, catalog=catalog, schema=schema)

    _log(f"Catalog  : {ns.catalog}")
    _log(f"Schema   : {ns.fqn}")

    catalog_attempted = False
    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {ns.catalog}")
        catalog_attempted = True
    except Exception as exc:
        msg = str(exc)
        if "PERMISSION_DENIED" in msg or "UNAUTHORIZED" in msg:
            fallback = current_catalog(spark)
            _log(f"No permission to create catalog '{ns.catalog}' — falling back to '{fallback}'")
            ns = resolve_namespace(spark, catalog=fallback, schema=ns.schema)
            _log(f"Catalog  : {ns.catalog}")
            _log(f"Schema   : {ns.fqn}")
        else:
            raise

    try:
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {ns.fqn} "
            f"COMMENT '{sql_string(DEFAULT_SCHEMA_COMMENT)}'"
        )
    except Exception as exc:
        msg = str(exc)
        if "PERMISSION_DENIED" in msg or "UNAUTHORIZED" in msg:
            fallback = current_catalog(spark)
            if ns.catalog != fallback:
                _log(f"No permission to create schema in '{ns.catalog}' — falling back to '{fallback}'")
                ns = resolve_namespace(spark, catalog=fallback, schema=ns.schema)
                _log(f"Catalog  : {ns.catalog}")
                _log(f"Schema   : {ns.fqn}")
                spark.sql(
                    f"CREATE SCHEMA IF NOT EXISTS {ns.fqn} "
                    f"COMMENT '{sql_string(DEFAULT_SCHEMA_COMMENT)}'"
                )
            else:
                raise
        else:
            raise
    _log(f"Schema ready: {ns.fqn}")

    sqls = build_table_sqls(ns.fqn, seed)
    tables: dict[str, int] = {}
    for name, sql in sqls.items():
        _log(f"Creating {name} ...")
        spark.sql(sql)
        cnt = spark.table(f"{ns.fqn}.{name}").count()
        tables[name] = cnt
        _log(f"  {name}: {cnt:,} rows")

    _log("Adding column comments ...")
    for table, cols in TABLE_COLUMN_COMMENTS.items():
        for col, comment in cols.items():
            spark.sql(
                f"ALTER TABLE {ns.fqn}.{table} "
                f"ALTER COLUMN {col} COMMENT '{sql_string(comment)}'"
            )
    _log("  Column comments applied")

    resolved_wh, skip_reason = resolve_warehouse_id(spark, warehouse_id)
    if skip_reason:
        _log(f"Genie: {skip_reason}")

    genie: GenieSpaceResult
    if resolved_wh:
        _log(f"Warehouse: {resolved_wh}")
        try:
            genie = create_or_replace_genie_space(
                spark, ns.fqn, resolved_wh, ns.username
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
        table_fqdns=table_fqdns(ns.fqn),
        warehouse_id=resolved_wh,
        genie=genie,
    )

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
    genie_button = (
        (_GENIE_BUTTON_HTML % {"genie_url": genie.url}) if genie.url else ""
    )
    _display_html(
        _SUMMARY_HTML
        % {"fqn": ns.fqn, "table_rows": table_rows, "total": f"{total:,}",
           "genie_button": genie_button}
    )

    return result.as_dict()


def teardown(spark: Any, **kwargs: Any) -> dict[str, Any]:
    """Remove all resources created by :func:`deploy`.

    The easiest way to call this is to unpack the dict returned by ``deploy``::

        result = deploy(spark)
        teardown(spark, **result)
    """
    return _cleanup(spark, deployment=kwargs)


__all__ = ["deploy", "teardown"]
