"""Package-wide configuration for the demand forecasting demo."""

from __future__ import annotations

PACKAGE_NAME = "demand-forecasting-genie"
PACKAGE_VERSION = "0.2.0"

DEFAULT_SEED = 20250306
DEFAULT_SCHEMA_BASENAME = "demand_forecasting"
DEFAULT_SCHEMA_COMMENT = (
    "NorthStar Logistics demand forecasting and inventory management demo data"
)

DEFAULT_SPACE_TITLE = "Northstar Logistics - Demand Forecasting & Inventory \U0001f69b"
SPACE_DESCRIPTION_MARKER = "Managed by demand_forecasting_genie"

AUTO_WAREHOUSE = "auto"
HTTP_TIMEOUT_SECONDS = 30

SHIPMENT_TABLE = "shipment_orders"
INVENTORY_TABLE = "inventory_levels"
FORECAST_TABLE = "demand_forecasts"

TABLE_NAMES = (
    FORECAST_TABLE,
    INVENTORY_TABLE,
    SHIPMENT_TABLE,
)
