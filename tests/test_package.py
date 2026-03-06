"""Fast contract tests for the demo package."""

from __future__ import annotations

import types
import unittest
from unittest.mock import patch

from demand_forecasting_genie.cleanup import cleanup
from demand_forecasting_genie.data import build_table_sqls
from demand_forecasting_genie.genie import (
    build_genie_payload,
    build_space_description,
    build_space_title,
    resolve_warehouse_id,
)
from demand_forecasting_genie.results import DeploymentResult, GenieSpaceResult
from demand_forecasting_genie.validators import default_schema_name, resolve_namespace


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


class PackageTests(unittest.TestCase):
    def test_default_schema_uses_user_scoped_name(self):
        self.assertEqual(
            default_schema_name("chad.macumber@databricks.com"),
            "demand_forecasting_chad_macumber",
        )

    def test_resolve_namespace_uses_current_catalog_and_user_schema(self):
        spark = _FakeSpark()
        namespace = resolve_namespace(spark, catalog=None, schema=None)
        self.assertEqual(namespace.catalog, "main")
        self.assertEqual(namespace.schema, "demand_forecasting_chad_macumber")
        self.assertEqual(namespace.fqn, "main.demand_forecasting_chad_macumber")

    def test_table_sqls_are_deterministic_and_seeded(self):
        sql_first = build_table_sqls("main.demo", seed=123)
        sql_second = build_table_sqls("main.demo", seed=123)
        sql_third = build_table_sqls("main.demo", seed=456)

        self.assertEqual(sql_first, sql_second)
        self.assertNotEqual(sql_first["shipment_orders"], sql_third["shipment_orders"])
        for statement in sql_first.values():
            self.assertNotIn("RAND(", statement)

        self.assertIn(
            "ABS(actual_demand - predicted_demand)",
            sql_first["demand_forecasts"],
        )

    def test_genie_payload_contains_managed_marker_and_sorted_tables(self):
        payload = build_genie_payload("main.demo", "warehouse-123", "chad.macumber@databricks.com")
        self.assertEqual(payload["title"], build_space_title("main.demo"))
        self.assertIn("fqn=main.demo", build_space_description("main.demo"))

        serialized = payload["serialized_space"]
        self.assertIn("main.demo.demand_forecasts", serialized)
        self.assertIn("main.demo.inventory_levels", serialized)
        self.assertIn("main.demo.shipment_orders", serialized)

        demand_pos = serialized.index("main.demo.demand_forecasts")
        inventory_pos = serialized.index("main.demo.inventory_levels")
        shipment_pos = serialized.index("main.demo.shipment_orders")
        self.assertLess(demand_pos, inventory_pos)
        self.assertLess(inventory_pos, shipment_pos)

    def test_deployment_result_preserves_genie_url(self):
        result = DeploymentResult(
            catalog="main",
            schema="demo",
            fqn="main.demo",
            seed=20250306,
            schema_created=True,
            catalog_attempted=False,
            tables={"shipment_orders": 1},
            table_fqdns={"shipment_orders": "main.demo.shipment_orders"},
            warehouse_id="warehouse-123",
            genie=GenieSpaceResult(
                status="created",
                requested=True,
                warehouse_id="warehouse-123",
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
            "demand_forecasting_genie.cleanup.find_managed_spaces",
            return_value=[{"space_id": "space-123"}],
        ), patch("demand_forecasting_genie.cleanup.delete_genie_space") as delete_mock:
            result = cleanup(spark, deployment=deployment)

        delete_mock.assert_called_once_with(spark, "space-123")
        self.assertTrue(result["dropped_schema"])
        self.assertEqual(result["deleted_space_ids"], ["space-123"])
        self.assertIn("DROP SCHEMA IF EXISTS main.demo_schema CASCADE", spark.executed_sql)

    def test_resolve_warehouse_id_prefers_running_serverless_like_warehouse(self):
        spark = _FakeSpark()
        payload = {
            "warehouses": [
                {"id": "slow", "name": "Analytics Warehouse", "state": "STOPPED", "cluster_size": "Small"},
                {"id": "best", "name": "Serverless Starter Warehouse", "state": "RUNNING", "cluster_size": "2X-Small"},
                {"id": "other", "name": "Shared Endpoint", "state": "RUNNING", "cluster_size": "X-Large"},
            ]
        }

        with patch("demand_forecasting_genie.genie._api_request", return_value=payload):
            warehouse_id, reason = resolve_warehouse_id(spark, "auto")

        self.assertEqual(warehouse_id, "best")
        self.assertIsNone(reason)


if __name__ == "__main__":
    unittest.main()
