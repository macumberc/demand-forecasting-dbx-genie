"""Genie payload construction and workspace API helpers."""

from __future__ import annotations

import json
import os
from typing import Any, Optional
import urllib.error
import urllib.request

from .config import AUTO_WAREHOUSE, HTTP_TIMEOUT_SECONDS, SPACE_DESCRIPTION_MARKER
from .results import GenieSpaceResult


def build_genie_payload(
    domain_spec: Any,
    fqn: str,
    warehouse_id: str,
    username: str,
) -> dict[str, Any]:
    """Build the Genie REST payload from a DomainSpec."""

    # Counter for generating unique IDs
    id_counter = [0]

    def next_id() -> str:
        id_counter[0] += 1
        return f"01f12000000000000000000000000{id_counter[0]:03d}"

    # Sample questions
    sample_questions = []
    for q in domain_spec.sample_questions:
        sample_questions.append({"id": next_id(), "question": [q]})

    # Data sources: base tables + metric views, sorted by identifier
    data_sources = []
    for table in domain_spec.tables:
        identifier = f"{fqn}.{table.table_name}"
        column_configs = []
        for col in table.columns:
            if col.is_dimension:
                if "date" in col.name.lower() or col.sql_type == "DATE":
                    column_configs.append(
                        {"column_name": col.name, "enable_format_assistance": True}
                    )
                else:
                    column_configs.append(
                        {
                            "column_name": col.name,
                            "enable_format_assistance": True,
                            "enable_entity_matching": True,
                        }
                    )
        entry: dict[str, Any] = {
            "identifier": identifier,
            "description": [table.description],
        }
        if column_configs:
            entry["column_configs"] = column_configs
        data_sources.append(entry)

    for mv in domain_spec.metric_views:
        identifier = f"{fqn}.{mv.view_name}"
        data_sources.append(
            {
                "identifier": identifier,
                "description": [
                    f"Metric view for {mv.source_table}. Use MEASURE() to query "
                    f"aggregated metrics."
                ],
            }
        )

    data_sources.sort(key=lambda x: x["identifier"])

    # Text instructions
    text_instructions = [
        {
            "id": next_id(),
            "content": [domain_spec.genie_instructions],
        }
    ]

    # Example question SQLs
    example_question_sqls = []
    for ex in domain_spec.example_sqls:
        sql_lines = _interpolate_fqn(ex.sql_lines, fqn)
        example_question_sqls.append(
            {"id": next_id(), "question": [ex.question], "sql": sql_lines}
        )

    # SQL snippets
    filters = []
    for f in domain_spec.sql_snippets.filters:
        filters.append(
            {
                "id": next_id(),
                "sql": [f["sql"]],
                "display_name": f["display_name"],
                "synonyms": f.get("synonyms", []),
                "instruction": [f.get("instruction", "")],
            }
        )

    expressions = []
    for e in domain_spec.sql_snippets.expressions:
        expressions.append(
            {
                "id": next_id(),
                "alias": e["alias"],
                "sql": [e["sql"]],
                "display_name": e["display_name"],
                "synonyms": e.get("synonyms", []),
            }
        )

    measures = []
    for m in domain_spec.sql_snippets.measures:
        measures.append(
            {
                "id": next_id(),
                "alias": m["alias"],
                "sql": [m["sql"]],
                "display_name": m["display_name"],
                "synonyms": m.get("synonyms", []),
            }
        )

    # Benchmarks
    benchmarks = []
    for b in domain_spec.benchmarks:
        sql_lines = _interpolate_fqn(b.sql_lines, fqn)
        benchmarks.append(
            {
                "id": next_id(),
                "question": [b.question],
                "answer": [{"format": "SQL", "content": sql_lines}],
            }
        )

    serialized_space = {
        "version": 2,
        "config": {"sample_questions": sample_questions},
        "data_sources": {"tables": data_sources},
        "instructions": {
            "text_instructions": text_instructions,
            "example_question_sqls": example_question_sqls,
            "join_specs": [],
            "sql_snippets": {
                "filters": filters,
                "expressions": expressions,
                "measures": measures,
            },
        },
        "benchmarks": {"questions": benchmarks},
    }

    description = (
        f"{SPACE_DESCRIPTION_MARKER}; fqn={fqn}\n\n"
        f"{domain_spec.space_description}"
    )

    return {
        "title": domain_spec.space_title,
        "description": description,
        "parent_path": f"/Workspace/Users/{username}",
        "warehouse_id": warehouse_id,
        "serialized_space": json.dumps(serialized_space),
    }


def _interpolate_fqn(sql_lines: list[str], fqn: str) -> list[str]:
    """Replace {fqn} placeholders in SQL lines."""
    return [line.replace("{fqn}", fqn) for line in sql_lines]


def resolve_warehouse_id(
    spark, warehouse_id: Optional[str]
) -> tuple[Optional[str], Optional[str]]:
    """Resolve a warehouse id or return a skip reason."""

    if warehouse_id in (None, ""):
        return None, "Genie creation skipped because no warehouse_id was provided."

    if warehouse_id != AUTO_WAREHOUSE:
        return warehouse_id, None

    try:
        data = _api_request(spark, "GET", "/api/2.0/sql/warehouses")
    except RuntimeError as exc:
        return None, f"Warehouse auto-discovery failed: {exc}"

    warehouses = data.get("warehouses", []) if isinstance(data, dict) else []
    if not warehouses:
        return None, "No accessible SQL warehouses were found."

    ordered = sorted(warehouses, key=_warehouse_sort_key)
    candidate = ordered[0]
    return candidate.get("id"), None


def create_or_replace_genie_space(
    spark,
    domain_spec: Any,
    fqn: str,
    warehouse_id: str,
    username: str,
) -> GenieSpaceResult:
    """Delete any prior managed space for the namespace and create a fresh one."""

    existing = find_managed_spaces(spark, fqn, domain_spec.space_title)
    replaced_ids: list[str] = []
    for space in existing:
        space_id = space.get("space_id")
        if space_id:
            delete_genie_space(spark, space_id)
            replaced_ids.append(space_id)

    payload = build_genie_payload(domain_spec, fqn, warehouse_id, username)
    created = _api_request(
        spark,
        "POST",
        "/api/2.0/genie/spaces",
        payload=payload,
        expected_statuses=(200, 201),
    )
    space_id = created["space_id"]
    workspace_url = _workspace_url(spark)

    return GenieSpaceResult(
        status="replaced" if replaced_ids else "created",
        requested=True,
        warehouse_id=warehouse_id,
        title=payload["title"],
        parent_path=payload["parent_path"],
        space_id=space_id,
        url=f"https://{workspace_url}/genie/rooms/{space_id}",
        replaced_space_ids=replaced_ids,
    )


def find_managed_spaces(spark, fqn: str, title: Optional[str] = None) -> list[dict[str, Any]]:
    """List spaces owned by this package for the target namespace."""

    data = _api_request(spark, "GET", "/api/2.0/genie/spaces")
    spaces = data.get("spaces", []) if isinstance(data, dict) else []
    marker = f"fqn={fqn}"

    results = []
    for space in spaces:
        description = space.get("description", "") or ""
        if marker in description or (title and space.get("title") == title):
            results.append(space)
    return results


def delete_genie_space(spark, space_id: str) -> None:
    """Delete a Genie space."""

    _api_request(
        spark,
        "DELETE",
        f"/api/2.0/genie/spaces/{space_id}",
        expected_statuses=(200, 202, 204),
    )


def _warehouse_sort_key(warehouse: dict[str, Any]) -> tuple[Any, ...]:
    """Prefer running, serverless-ish, and smaller warehouses."""

    size_rank = {
        "2X-Small": 0,
        "X-Small": 1,
        "Small": 2,
        "Medium": 3,
        "Large": 4,
        "X-Large": 5,
        "2X-Large": 6,
    }
    name = (warehouse.get("name") or "").lower()
    return (
        warehouse.get("state") != "RUNNING",
        "serverless" not in name,
        "starter" not in name,
        "shared" not in name,
        size_rank.get(warehouse.get("cluster_size"), 99),
        name,
    )


def _api_request(
    spark,
    method: str,
    path: str,
    payload: Optional[dict[str, Any]] = None,
    expected_statuses: tuple[int, ...] = (200,),
) -> Any:
    """Issue a Databricks workspace REST request using notebook auth."""

    workspace_url = _workspace_url(spark)
    token = _api_token(spark)

    request_body = None
    if payload is not None:
        request_body = json.dumps(payload).encode("utf-8")

    request = urllib.request.Request(
        url=f"https://{workspace_url}{path}",
        data=request_body,
        method=method,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )

    try:
        with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
            status_code = response.getcode()
            response_text = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"{method} {path} failed with status {exc.code}: {error_body}"
        ) from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"{method} {path} failed: {exc}") from exc

    if status_code not in expected_statuses:
        raise RuntimeError(
            f"{method} {path} failed with status {status_code}: {response_text}"
        )

    if not response_text:
        return {}
    return json.loads(response_text)


def _workspace_url(spark) -> str:
    """Resolve the current workspace URL without protocol."""

    try:
        return spark.conf.get("spark.databricks.workspaceUrl")
    except Exception as exc:
        raise RuntimeError(
            f"Could not resolve spark.databricks.workspaceUrl: {exc}"
        ) from exc


def _api_token(spark) -> str:
    """Resolve an API token from dbutils or the environment."""

    dbutils = _get_dbutils(spark)
    if dbutils is not None:
        try:
            return (
                dbutils.notebook.entry_point.getDbutils()
                .notebook()
                .getContext()
                .apiToken()
                .get()
            )
        except Exception:
            pass

    token = os.environ.get("DATABRICKS_TOKEN")
    if token:
        return token

    raise RuntimeError(
        "Could not obtain a Databricks API token from dbutils or DATABRICKS_TOKEN."
    )


def _get_dbutils(spark):
    """Resolve dbutils when running inside a Databricks environment."""

    try:
        from pyspark.dbutils import DBUtils

        return DBUtils(spark)
    except Exception:
        try:
            import IPython

            return IPython.get_ipython().user_ns.get("dbutils")
        except Exception:
            return None
