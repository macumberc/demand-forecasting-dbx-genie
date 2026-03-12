"""Genie payload construction and workspace API helpers."""

from __future__ import annotations

import json
import os
from typing import Any, Optional
import urllib.error
import urllib.request

from .config import (
    AUTO_WAREHOUSE,
    DEFAULT_SPACE_TITLE,
    FORECAST_METRICS_VIEW,
    FORECAST_TABLE,
    HTTP_TIMEOUT_SECONDS,
    INVENTORY_METRICS_VIEW,
    INVENTORY_TABLE,
    SHIPMENT_METRICS_VIEW,
    SHIPMENT_TABLE,
    SPACE_DESCRIPTION_MARKER,
)
from .results import GenieSpaceResult


def build_space_title(fqn: str) -> str:
    """Render a user-facing Genie title."""

    return DEFAULT_SPACE_TITLE


def build_space_description(fqn: str) -> str:
    """Build a managed-space description with an embedded marker."""

    return (
        f"{SPACE_DESCRIPTION_MARKER}; fqn={fqn}\n\n"
        "Supply chain analytics for NorthStar Logistics. Ask questions about "
        "demand forecasts, inventory levels, stockout risks, fill rates, and "
        "shipment trends across 20 products and 8 distribution centers."
    )


def build_genie_payload(fqn: str, warehouse_id: str, username: str) -> dict[str, Any]:
    """Build the Genie REST payload."""

    serialized_space = {
        "version": 2,
        "config": {
            "sample_questions": [
                {
                    "id": "01f12000000000000000000000000001",
                    "question": ["Which SKUs are currently below their reorder point?"],
                },
                {
                    "id": "01f12000000000000000000000000002",
                    "question": ["What is the forecast accuracy by product category?"],
                },
                {
                    "id": "01f12000000000000000000000000003",
                    "question": ["Show shipment volume by warehouse over the last 3 months"],
                },
                {
                    "id": "01f12000000000000000000000000004",
                    "question": ["Which warehouses have fill rates below 95%?"],
                },
                {
                    "id": "01f12000000000000000000000000005",
                    "question": ["What is the average days of supply by product category?"],
                },
                {
                    "id": "01f12000000000000000000000000006",
                    "question": ["What is our fill rate by product category?"],
                },
                {
                    "id": "01f12000000000000000000000000007",
                    "question": ["Show total revenue by destination region"],
                },
            ]
        },
        "data_sources": {
            "tables": [
                {
                    "identifier": f"{fqn}.{FORECAST_TABLE}",
                    "description": [
                        "Monthly demand forecasts, confidence intervals, actual demand, "
                        "and forecast error by product and region."
                    ],
                    "column_configs": [
                        {"column_name": "forecast_date", "enable_format_assistance": True},
                        {
                            "column_name": "model_version",
                            "enable_format_assistance": True,
                            "enable_entity_matching": True,
                        },
                        {
                            "column_name": "product_category",
                            "enable_format_assistance": True,
                            "enable_entity_matching": True,
                        },
                        {
                            "column_name": "region",
                            "enable_format_assistance": True,
                            "enable_entity_matching": True,
                        },
                    ],
                },
                {
                    "identifier": f"{fqn}.{FORECAST_METRICS_VIEW}",
                    "description": [
                        "Metric view for demand forecasts. Use MEASURE() to query "
                        "total predicted/actual demand, forecast accuracy, and variance."
                    ],
                },
                {
                    "identifier": f"{fqn}.{INVENTORY_TABLE}",
                    "description": [
                        "Weekly inventory snapshots across 8 distribution centers. Tracks "
                        "quantity on hand, reorder points, safety stock, and lead times."
                    ],
                    "column_configs": [
                        {
                            "column_name": "product_category",
                            "enable_format_assistance": True,
                            "enable_entity_matching": True,
                        },
                        {
                            "column_name": "product_sku",
                            "enable_format_assistance": True,
                            "enable_entity_matching": True,
                        },
                        {
                            "column_name": "warehouse_id",
                            "enable_format_assistance": True,
                            "enable_entity_matching": True,
                        },
                        {
                            "column_name": "warehouse_name",
                            "enable_format_assistance": True,
                            "enable_entity_matching": True,
                        },
                    ],
                },
                {
                    "identifier": f"{fqn}.{INVENTORY_METRICS_VIEW}",
                    "description": [
                        "Metric view for inventory levels. Use MEASURE() to query "
                        "quantity on hand, inventory value, and stockout risk counts."
                    ],
                },
                {
                    "identifier": f"{fqn}.{SHIPMENT_TABLE}",
                    "description": [
                        "Daily shipment order history with delivered units, warehouse "
                        "origins, destination regions, and order outcomes."
                    ],
                    "column_configs": [
                        {
                            "column_name": "destination_region",
                            "enable_format_assistance": True,
                            "enable_entity_matching": True,
                        },
                        {"column_name": "order_date", "enable_format_assistance": True},
                        {
                            "column_name": "order_status",
                            "enable_format_assistance": True,
                            "enable_entity_matching": True,
                        },
                        {
                            "column_name": "product_category",
                            "enable_format_assistance": True,
                            "enable_entity_matching": True,
                        },
                        {
                            "column_name": "product_sku",
                            "enable_format_assistance": True,
                            "enable_entity_matching": True,
                        },
                        {
                            "column_name": "warehouse_id",
                            "enable_format_assistance": True,
                            "enable_entity_matching": True,
                        },
                    ],
                },
                {
                    "identifier": f"{fqn}.{SHIPMENT_METRICS_VIEW}",
                    "description": [
                        "Metric view for shipment orders. Use MEASURE() to query "
                        "total revenue, fill rate, units shipped, and order counts."
                    ],
                },
            ]
        },
        "instructions": {
            "text_instructions": [
                {
                    "id": "01f12000000000000000000000000011",
                    "content": [
                        "You are a supply chain analytics assistant for NorthStar Logistics, ",
                        "a nationwide 3PL provider with 8 regional distribution centers.\n\n",
                        "For current inventory questions, always filter to the latest snapshot_date in inventory_levels.\n",
                        "Round percentages to 1 decimal place and monetary values to 2 decimal places.\n",
                        "When computing daily demand from shipment_orders, filter to order_status = 'Delivered'.\n",
                        "When answering aggregation questions (totals, averages, counts, ratios), prefer the metric views ",
                        f"({SHIPMENT_METRICS_VIEW}, {INVENTORY_METRICS_VIEW}, {FORECAST_METRICS_VIEW}) ",
                        "with MEASURE() syntax instead of raw SQL aggregations on the base tables.\n",
                    ],
                }
            ],
            "example_question_sqls": [
                {
                    "id": "01f12000000000000000000000000021",
                    "question": ["Which SKUs are currently below their reorder point?"],
                    "sql": [
                        f"WITH latest_inventory AS (\n",
                        f"  SELECT *\n",
                        f"  FROM {fqn}.{INVENTORY_TABLE}\n",
                        f"  WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM {fqn}.{INVENTORY_TABLE})\n",
                        f")\n",
                        f"SELECT product_sku, product_name, warehouse_id, warehouse_name,\n",
                        f"  quantity_on_hand, reorder_point, safety_stock_qty,\n",
                        f"  CASE\n",
                        f"    WHEN quantity_on_hand < safety_stock_qty THEN 'CRITICAL'\n",
                        f"    WHEN quantity_on_hand <= reorder_point THEN 'LOW'\n",
                        f"    ELSE 'HEALTHY'\n",
                        f"  END AS stock_status\n",
                        f"FROM latest_inventory\n",
                        f"WHERE quantity_on_hand <= reorder_point\n",
                        f"ORDER BY quantity_on_hand / NULLIF(reorder_point, 0) ASC"
                    ],
                },
                {
                    "id": "01f12000000000000000000000000022",
                    "question": ["Show shipment volume by warehouse over the last 3 months"],
                    "sql": [
                        f"SELECT warehouse_id,\n",
                        f"  SUM(quantity) AS total_units_shipped,\n",
                        f"  COUNT(DISTINCT order_id) AS total_orders\n",
                        f"FROM {fqn}.{SHIPMENT_TABLE}\n",
                        f"WHERE order_date >= ADD_MONTHS((SELECT MAX(order_date) FROM {fqn}.{SHIPMENT_TABLE}), -3)\n",
                        f"  AND order_status = 'Delivered'\n",
                        f"GROUP BY warehouse_id\n",
                        f"ORDER BY total_units_shipped DESC"
                    ],
                },
                {
                    "id": "01f12000000000000000000000000023",
                    "question": ["What is the forecast accuracy by product category?"],
                    "sql": [
                        f"SELECT product_category,\n",
                        f"  ROUND(AVG(100 - forecast_error_pct), 1) AS avg_accuracy_pct,\n",
                        f"  ROUND(AVG(forecast_error_pct), 1) AS avg_error_pct,\n",
                        f"  COUNT(*) AS num_forecasts\n",
                        f"FROM {fqn}.{FORECAST_TABLE}\n",
                        f"GROUP BY product_category\n",
                        f"ORDER BY avg_accuracy_pct DESC"
                    ],
                },
                {
                    "id": "01f12000000000000000000000000024",
                    "question": ["What is the average days of supply by product category?"],
                    "sql": [
                        f"WITH latest_inventory AS (\n",
                        f"  SELECT *\n",
                        f"  FROM {fqn}.{INVENTORY_TABLE}\n",
                        f"  WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM {fqn}.{INVENTORY_TABLE})\n",
                        f"), daily_demand AS (\n",
                        f"  SELECT product_sku, warehouse_id,\n",
                        f"    SUM(quantity) / NULLIF(DATEDIFF(MAX(order_date), MIN(order_date)), 0) AS avg_daily_demand\n",
                        f"  FROM {fqn}.{SHIPMENT_TABLE}\n",
                        f"  WHERE order_status = 'Delivered'\n",
                        f"  GROUP BY product_sku, warehouse_id\n",
                        f")\n",
                        f"SELECT i.product_category,\n",
                        f"  ROUND(AVG(i.quantity_on_hand / NULLIF(d.avg_daily_demand, 0)), 1) AS avg_days_of_supply\n",
                        f"FROM latest_inventory i\n",
                        f"LEFT JOIN daily_demand d\n",
                        f"  ON i.product_sku = d.product_sku AND i.warehouse_id = d.warehouse_id\n",
                        f"GROUP BY i.product_category\n",
                        f"ORDER BY avg_days_of_supply ASC"
                    ],
                },
                {
                    "id": "01f12000000000000000000000000025",
                    "question": ["Compare actual vs forecasted demand for Electronics in Q4"],
                    "sql": [
                        f"SELECT product_name, forecast_date,\n",
                        f"  predicted_demand, actual_demand,\n",
                        f"  actual_demand - predicted_demand AS variance,\n",
                        f"  ROUND(100 - forecast_error_pct, 1) AS accuracy_pct,\n",
                        f"  model_version, region\n",
                        f"FROM {fqn}.{FORECAST_TABLE}\n",
                        f"WHERE product_category = 'Electronics'\n",
                        f"  AND forecast_date >= DATE'2025-10-01'\n",
                        f"ORDER BY forecast_date, product_name, region"
                    ],
                },
                {
                    "id": "01f12000000000000000000000000026",
                    "question": ["What is our fill rate by product category?"],
                    "sql": [
                        f"SELECT product_category,\n",
                        f"  MEASURE(fill_rate) AS fill_rate\n",
                        f"FROM {fqn}.{SHIPMENT_METRICS_VIEW}\n",
                        f"GROUP BY ALL\n",
                        f"ORDER BY fill_rate ASC"
                    ],
                },
                {
                    "id": "01f12000000000000000000000000027",
                    "question": ["Show total revenue by destination region"],
                    "sql": [
                        f"SELECT destination_region,\n",
                        f"  MEASURE(total_revenue) AS total_revenue\n",
                        f"FROM {fqn}.{SHIPMENT_METRICS_VIEW}\n",
                        f"GROUP BY ALL\n",
                        f"ORDER BY total_revenue DESC"
                    ],
                },
            ],
            "join_specs": [],
            "sql_snippets": {
                "filters": [
                    {
                        "id": "01f12000000000000000000000000031",
                        "sql": ["inventory.quantity_on_hand <= inventory.reorder_point"],
                        "display_name": "below reorder point",
                        "synonyms": ["low stock", "needs reorder", "stockout risk"],
                        "instruction": ["Use for products that need replenishment."],
                    },
                    {
                        "id": "01f12000000000000000000000000032",
                        "sql": ["inventory.quantity_on_hand < inventory.safety_stock_qty"],
                        "display_name": "below safety stock",
                        "synonyms": ["critical stock", "emergency", "urgent reorder"],
                        "instruction": ["Use for critically low inventory situations."],
                    },
                    {
                        "id": "01f12000000000000000000000000033",
                        "sql": ["orders.order_status = 'Delivered'"],
                        "display_name": "delivered orders",
                        "synonyms": ["completed orders", "fulfilled orders"],
                        "instruction": ["Use when calculating fill rate or shipment volume."],
                    },
                ],
                "expressions": [
                    {
                        "id": "01f12000000000000000000000000041",
                        "alias": "order_month",
                        "sql": ["DATE_TRUNC('month', orders.order_date)"],
                        "display_name": "month",
                        "synonyms": ["order month", "monthly"],
                    },
                    {
                        "id": "01f12000000000000000000000000042",
                        "alias": "order_quarter",
                        "sql": ["DATE_TRUNC('quarter', orders.order_date)"],
                        "display_name": "quarter",
                        "synonyms": ["order quarter", "quarterly"],
                    },
                    {
                        "id": "01f12000000000000000000000000043",
                        "alias": "stock_status",
                        "sql": [
                            "CASE WHEN inventory.quantity_on_hand < inventory.safety_stock_qty THEN 'Critical' "
                            "WHEN inventory.quantity_on_hand <= inventory.reorder_point THEN 'Low' "
                            "WHEN inventory.quantity_on_hand <= inventory.reorder_point * 2 THEN 'Healthy' "
                            "ELSE 'Overstocked' END"
                        ],
                        "display_name": "stock status",
                        "synonyms": ["inventory status", "stock level classification"],
                    },
                ],
                "measures": [
                    {
                        "id": "01f12000000000000000000000000051",
                        "alias": "total_shipped",
                        "sql": ["SUM(orders.quantity)"],
                        "display_name": "total units shipped",
                        "synonyms": ["shipment volume", "units shipped"],
                    },
                    {
                        "id": "01f12000000000000000000000000052",
                        "alias": "total_revenue",
                        "sql": ["SUM(orders.quantity * orders.unit_price)"],
                        "display_name": "total revenue",
                        "synonyms": ["revenue", "sales"],
                    },
                    {
                        "id": "01f12000000000000000000000000053",
                        "alias": "avg_forecast_accuracy",
                        "sql": ["ROUND(AVG(100 - forecasts.forecast_error_pct), 1)"],
                        "display_name": "average forecast accuracy",
                        "synonyms": ["forecast accuracy", "model accuracy"],
                    },
                    {
                        "id": "01f12000000000000000000000000054",
                        "alias": "total_inventory_value",
                        "sql": ["SUM(inventory.quantity_on_hand * inventory.unit_cost)"],
                        "display_name": "total inventory value",
                        "synonyms": ["inventory value", "stock value"],
                    },
                ],
            },
        },
        "benchmarks": {
            "questions": [
                {
                    "id": "01f12000000000000000000000000061",
                    "question": ["Which SKUs are currently below their reorder point?"],
                    "answer": [
                        {
                            "format": "SQL",
                            "content": [
                                f"WITH latest_inventory AS (\n",
                                f"  SELECT * FROM {fqn}.{INVENTORY_TABLE}\n",
                                f"  WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM {fqn}.{INVENTORY_TABLE})\n",
                                f")\n",
                                f"SELECT product_sku, product_name, warehouse_id, warehouse_name, quantity_on_hand, reorder_point\n",
                                f"FROM latest_inventory\n",
                                f"WHERE quantity_on_hand <= reorder_point\n",
                                f"ORDER BY quantity_on_hand ASC"
                            ],
                        }
                    ],
                },
                {
                    "id": "01f12000000000000000000000000062",
                    "question": ["What is the forecast accuracy by product category?"],
                    "answer": [
                        {
                            "format": "SQL",
                            "content": [
                                f"SELECT product_category, ROUND(AVG(100 - forecast_error_pct), 1) AS accuracy_pct\n",
                                f"FROM {fqn}.{FORECAST_TABLE}\n",
                                f"GROUP BY product_category\n",
                                f"ORDER BY accuracy_pct DESC"
                            ],
                        }
                    ],
                },
                {
                    "id": "01f12000000000000000000000000063",
                    "question": ["Show shipment volume by warehouse"],
                    "answer": [
                        {
                            "format": "SQL",
                            "content": [
                                f"SELECT warehouse_id, SUM(quantity) AS total_shipped\n",
                                f"FROM {fqn}.{SHIPMENT_TABLE}\n",
                                f"WHERE order_status = 'Delivered'\n",
                                f"GROUP BY warehouse_id\n",
                                f"ORDER BY total_shipped DESC"
                            ],
                        }
                    ],
                },
                {
                    "id": "01f12000000000000000000000000064",
                    "question": ["What is the total inventory value by warehouse?"],
                    "answer": [
                        {
                            "format": "SQL",
                            "content": [
                                f"WITH latest_inventory AS (\n",
                                f"  SELECT * FROM {fqn}.{INVENTORY_TABLE}\n",
                                f"  WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM {fqn}.{INVENTORY_TABLE})\n",
                                f")\n",
                                f"SELECT warehouse_id, warehouse_name,\n",
                                f"  ROUND(SUM(quantity_on_hand * unit_cost), 2) AS total_value\n",
                                f"FROM latest_inventory\n",
                                f"GROUP BY warehouse_id, warehouse_name\n",
                                f"ORDER BY total_value DESC"
                            ],
                        }
                    ],
                },
                {
                    "id": "01f12000000000000000000000000065",
                    "question": ["Which products need reordering today based on lead time?"],
                    "answer": [
                        {
                            "format": "SQL",
                            "content": [
                                f"WITH latest_inventory AS (\n",
                                f"  SELECT * FROM {fqn}.{INVENTORY_TABLE}\n",
                                f"  WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM {fqn}.{INVENTORY_TABLE})\n",
                                f")\n",
                                f"SELECT product_sku, product_name, warehouse_name,\n",
                                f"  quantity_on_hand, reorder_point, lead_time_days\n",
                                f"FROM latest_inventory\n",
                                f"WHERE quantity_on_hand <= reorder_point\n",
                                f"ORDER BY lead_time_days DESC, quantity_on_hand ASC"
                            ],
                        }
                    ],
                },
                {
                    "id": "01f12000000000000000000000000066",
                    "question": ["What is our fill rate by product category?"],
                    "answer": [
                        {
                            "format": "SQL",
                            "content": [
                                f"SELECT product_category,\n",
                                f"  MEASURE(fill_rate) AS fill_rate\n",
                                f"FROM {fqn}.{SHIPMENT_METRICS_VIEW}\n",
                                f"GROUP BY ALL\n",
                                f"ORDER BY fill_rate ASC"
                            ],
                        }
                    ],
                },
                {
                    "id": "01f12000000000000000000000000067",
                    "question": ["Show total revenue by destination region"],
                    "answer": [
                        {
                            "format": "SQL",
                            "content": [
                                f"SELECT destination_region,\n",
                                f"  MEASURE(total_revenue) AS total_revenue\n",
                                f"FROM {fqn}.{SHIPMENT_METRICS_VIEW}\n",
                                f"GROUP BY ALL\n",
                                f"ORDER BY total_revenue DESC"
                            ],
                        }
                    ],
                },
            ]
        },
    }

    return {
        "title": build_space_title(fqn),
        "description": build_space_description(fqn),
        "parent_path": f"/Workspace/Users/{username}",
        "warehouse_id": warehouse_id,
        "serialized_space": json.dumps(serialized_space),
    }


def resolve_warehouse_id(spark, warehouse_id: Optional[str]) -> tuple[Optional[str], Optional[str]]:
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
    fqn: str,
    warehouse_id: str,
    username: str,
) -> GenieSpaceResult:
    """Delete any prior managed space for the namespace and create a fresh one."""

    existing = find_managed_spaces(spark, fqn)
    replaced_ids: list[str] = []
    for space in existing:
        space_id = space.get("space_id")
        if space_id:
            delete_genie_space(spark, space_id)
            replaced_ids.append(space_id)

    payload = build_genie_payload(fqn, warehouse_id, username)
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


def find_managed_spaces(spark, fqn: str) -> list[dict[str, Any]]:
    """List spaces owned by this package for the target namespace."""

    data = _api_request(spark, "GET", "/api/2.0/genie/spaces")
    spaces = data.get("spaces", []) if isinstance(data, dict) else []
    marker = f"fqn={fqn}"
    title = build_space_title(fqn)

    results = []
    for space in spaces:
        description = space.get("description", "") or ""
        if marker in description or space.get("title") == title:
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
        raise RuntimeError(f"Could not resolve spark.databricks.workspaceUrl: {exc}") from exc


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
