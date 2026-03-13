"""Contract tests for the genie-space-generator package."""

from __future__ import annotations

import types
import unittest
from unittest.mock import patch

from genie_space_generator.cleanup import cleanup
from genie_space_generator.data import (
    build_metric_view_sqls_from_spec,
    build_table_sqls_from_spec,
    metric_view_fqdns,
)
from genie_space_generator.genie import build_genie_payload, resolve_warehouse_id
from genie_space_generator.generator import (
    BenchmarkSpec,
    ColumnSpec,
    DomainSpec,
    ExampleSQL,
    MetricViewSpec,
    SQLSnippets,
    TableSpec,
)
from genie_space_generator.results import DeploymentResult, GenieSpaceResult
from genie_space_generator.validators import default_schema_name, resolve_namespace


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_domain_spec() -> DomainSpec:
    """Create a minimal valid DomainSpec for testing."""

    columns = [
        ColumnSpec("order_id", "STRING", "Unique order ID", False,
                    "CONCAT('ORD-', LPAD(CAST(ROW_NUMBER() OVER (ORDER BY d.dt, e.sku) + 10000 AS STRING), 6, '0'))"),
        ColumnSpec("order_date", "DATE", "Date order was placed", True, "d.dt"),
        ColumnSpec("product_sku", "STRING", "Product SKU", True, ""),
        ColumnSpec("product_name", "STRING", "Product name", True, ""),
        ColumnSpec("product_category", "STRING", "Product category", True, ""),
        ColumnSpec("quantity", "INT", "Units ordered", False,
                    "CAST(10 + FLOOR({qty_noise} * 100) AS INT)"),
        ColumnSpec("unit_price", "DOUBLE", "Price per unit", False, ""),
        ColumnSpec("order_status", "STRING", "Order status", True,
                    "CASE WHEN {status_noise} < 0.02 THEN 'Cancelled' ELSE 'Delivered' END"),
    ]
    table = TableSpec(
        table_name="test_orders",
        description="Test order transactions.",
        columns=columns,
        seasonal_patterns={"Electronics": {"11,12": 0.06}},
        entity_dimension="transaction",
        dimension_values=[
            {"sku": "SKU-001", "name": "Widget A", "category": "Electronics", "price": 29.99},
            {"sku": "SKU-002", "name": "Widget B", "category": "Electronics", "price": 49.99},
        ],
        category_distribution={"Electronics": 0.025},
    )
    mv = MetricViewSpec(
        view_name="test_orders_metrics",
        source_table="test_orders",
        dimensions=[{"name": "order_date", "expr": "order_date"}],
        measures=[
            {"name": "total_revenue", "expr": "SUM(quantity * unit_price)", "comment": "Total revenue"},
            {"name": "fill_rate", "expr": "COUNT(1)", "comment": "Fill rate"},
        ],
    )
    example_sqls = [
        ExampleSQL("Q1?", ["SELECT 1"]),
        ExampleSQL("Q2?", ["SELECT 2"]),
        ExampleSQL("Q3?", ["SELECT 3"]),
        ExampleSQL("Q4?", ["SELECT 4"]),
        ExampleSQL("Q5?", ["SELECT 5"]),
        ExampleSQL("Q6 measure?", ["SELECT MEASURE(total_revenue) FROM {fqn}.test_orders_metrics GROUP BY ALL"]),
        ExampleSQL("Q7 measure?", ["SELECT MEASURE(fill_rate) FROM {fqn}.test_orders_metrics GROUP BY ALL"]),
    ]
    benchmarks = [
        BenchmarkSpec("B1?", ["SELECT 1"]),
        BenchmarkSpec("B2?", ["SELECT 2"]),
        BenchmarkSpec("B3?", ["SELECT 3"]),
        BenchmarkSpec("B4?", ["SELECT 4"]),
        BenchmarkSpec("B5?", ["SELECT 5"]),
        BenchmarkSpec("B6 measure?", ["SELECT MEASURE(total_revenue) FROM {fqn}.test_orders_metrics GROUP BY ALL"]),
        BenchmarkSpec("B7 measure?", ["SELECT MEASURE(fill_rate) FROM {fqn}.test_orders_metrics GROUP BY ALL"]),
    ]
    return DomainSpec(
        company_name="TestCo",
        industry="Testing",
        use_case="Unit testing",
        space_title="TestCo - Unit Testing",
        space_description="Test description.",
        schema_basename="test_demo",
        tables=[table],
        metric_views=[mv],
        genie_instructions="You are a test assistant.",
        sample_questions=["Q1?", "Q2?", "Q3?", "Q4?", "Q5?", "Q6?", "Q7?"],
        example_sqls=example_sqls,
        sql_snippets=SQLSnippets(
            filters=[
                {"sql": "status = 'Active'", "display_name": "active", "synonyms": [], "instruction": ""},
                {"sql": "status = 'Closed'", "display_name": "closed", "synonyms": [], "instruction": ""},
                {"sql": "qty > 0", "display_name": "has quantity", "synonyms": [], "instruction": ""},
            ],
            expressions=[
                {"alias": "month", "sql": "DATE_TRUNC('month', dt)", "display_name": "month", "synonyms": []},
                {"alias": "quarter", "sql": "DATE_TRUNC('quarter', dt)", "display_name": "quarter", "synonyms": []},
                {"alias": "status_label", "sql": "CASE WHEN x THEN 'Y' END", "display_name": "status", "synonyms": []},
            ],
            measures=[
                {"alias": "total", "sql": "SUM(qty)", "display_name": "total", "synonyms": []},
                {"alias": "avg_val", "sql": "AVG(val)", "display_name": "average", "synonyms": []},
                {"alias": "cnt", "sql": "COUNT(*)", "display_name": "count", "synonyms": []},
                {"alias": "max_val", "sql": "MAX(val)", "display_name": "max", "synonyms": []},
            ],
        ),
        benchmarks=benchmarks,
    )


class _FakeQueryResult:
    def __init__(self, first_value=None, rows=None):
        self._first_value = first_value
        self._rows = rows or []

    def first(self):
        return (self._first_value,)

    def collect(self):
        return list(self._rows)


class _FakeSpark:
    def __init__(self, current_catalog="main", current_user="chad.macumber@databricks.com"):
        self._current_catalog = current_catalog
        self._current_user = current_user
        self.executed_sql = []
        self.catalog = types.SimpleNamespace(tableExists=lambda _: False)

    def sql(self, query):
        self.executed_sql.append(query)
        if "current_catalog()" in query:
            return _FakeQueryResult(self._current_catalog)
        if "current_user()" in query:
            return _FakeQueryResult(self._current_user)
        return _FakeQueryResult(rows=[])


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class PackageTests(unittest.TestCase):
    def test_default_schema_uses_basename_and_user_slug(self):
        self.assertEqual(
            default_schema_name("chad.macumber@databricks.com", "medflow_readmissions"),
            "medflow_readmissions_chad_macumber",
        )

    def test_resolve_namespace_uses_custom_basename(self):
        spark = _FakeSpark()
        namespace = resolve_namespace(spark, catalog=None, schema=None, schema_basename="my_demo")
        self.assertEqual(namespace.catalog, "main")
        self.assertEqual(namespace.schema, "my_demo_chad_macumber")
        self.assertEqual(namespace.fqn, "main.my_demo_chad_macumber")

    def test_table_sqls_are_deterministic(self):
        spec = _make_domain_spec()
        sql_first = build_table_sqls_from_spec(spec, "main.demo", seed=123)
        sql_second = build_table_sqls_from_spec(spec, "main.demo", seed=123)
        sql_third = build_table_sqls_from_spec(spec, "main.demo", seed=456)

        self.assertEqual(sql_first, sql_second)
        self.assertNotEqual(
            sql_first["test_orders"], sql_third["test_orders"]
        )
        for statement in sql_first.values():
            self.assertNotIn("RAND(", statement)

    def test_genie_payload_contains_marker_and_tables(self):
        spec = _make_domain_spec()
        payload = build_genie_payload(spec, "main.demo", "wh-123", "user@databricks.com")
        self.assertEqual(payload["title"], "TestCo - Unit Testing")
        self.assertIn("fqn=main.demo", payload["description"])

        serialized = payload["serialized_space"]
        self.assertIn("main.demo.test_orders", serialized)
        self.assertIn("main.demo.test_orders_metrics", serialized)
        self.assertIn("MEASURE(", serialized)

    def test_deployment_result_preserves_genie_url(self):
        result = DeploymentResult(
            catalog="main",
            schema="demo",
            fqn="main.demo",
            seed=20250306,
            schema_created=True,
            catalog_attempted=False,
            tables={"test_orders": 1},
            table_fqdns={"test_orders": "main.demo.test_orders"},
            warehouse_id="wh-123",
            genie=GenieSpaceResult(
                status="created",
                requested=True,
                warehouse_id="wh-123",
                space_id="space-123",
                url="https://example/genie/space-123",
            ),
        ).as_dict()

        self.assertEqual(result["genie_url"], "https://example/genie/space-123")
        self.assertEqual(result["genie"]["space_id"], "space-123")

    def test_cleanup_drops_schema_and_deletes_managed_space(self):
        spark = _FakeSpark()
        deployment = {
            "catalog": "main",
            "schema": "demo_schema",
            "fqn": "main.demo_schema",
        }

        with patch(
            "genie_space_generator.cleanup.find_managed_spaces",
            return_value=[{"space_id": "space-123"}],
        ), patch("genie_space_generator.cleanup.delete_genie_space") as delete_mock:
            result = cleanup(spark, deployment=deployment)

        delete_mock.assert_called_once_with(spark, "space-123")
        self.assertTrue(result["dropped_schema"])
        self.assertEqual(result["deleted_space_ids"], ["space-123"])
        self.assertIn("DROP SCHEMA IF EXISTS main.demo_schema CASCADE", spark.executed_sql)

    def test_metric_view_sqls_contain_with_metrics_syntax(self):
        spec = _make_domain_spec()
        sqls = build_metric_view_sqls_from_spec(spec, "main.demo")
        for name, sql in sqls.items():
            self.assertIn("WITH METRICS", sql, f"{name} missing WITH METRICS")
            self.assertIn("LANGUAGE YAML", sql, f"{name} missing LANGUAGE YAML")
            self.assertIn("version: 1.1", sql, f"{name} missing version: 1.1")
            self.assertIn("main.demo.", sql, f"{name} missing source reference")

    def test_metric_view_fqdns(self):
        spec = _make_domain_spec()
        fqdns = metric_view_fqdns(spec, "main.demo")
        self.assertIsInstance(fqdns, dict)
        self.assertEqual(len(fqdns), 1)
        for name, fqdn in fqdns.items():
            self.assertTrue(fqdn.startswith("main.demo."))
            self.assertIn("metrics", fqdn)

    def test_resolve_warehouse_id_prefers_running_serverless(self):
        spark = _FakeSpark()
        payload = {
            "warehouses": [
                {"id": "slow", "name": "Analytics Warehouse", "state": "STOPPED", "cluster_size": "Small"},
                {"id": "best", "name": "Serverless Starter Warehouse", "state": "RUNNING", "cluster_size": "2X-Small"},
                {"id": "other", "name": "Shared Endpoint", "state": "RUNNING", "cluster_size": "X-Large"},
            ]
        }

        with patch("genie_space_generator.genie._api_request", return_value=payload):
            warehouse_id, reason = resolve_warehouse_id(spark, "auto")

        self.assertEqual(warehouse_id, "best")
        self.assertIsNone(reason)


if __name__ == "__main__":
    unittest.main()
