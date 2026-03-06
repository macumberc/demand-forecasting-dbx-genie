"""Deterministic SQL builders and domain metadata."""

from __future__ import annotations

from .config import FORECAST_TABLE, INVENTORY_TABLE, SHIPMENT_TABLE

PRODUCTS = [
    {
        "sku": "SKU-EL-001",
        "name": "Wireless Bluetooth Speaker",
        "category": "Electronics",
        "price": 49.99,
        "unit_cost": 28.50,
        "reorder_point": 100,
        "safety_stock": 50,
    },
    {
        "sku": "SKU-EL-003",
        "name": "USB-C Hub 7-in-1",
        "category": "Electronics",
        "price": 39.99,
        "unit_cost": 22.00,
        "reorder_point": 75,
        "safety_stock": 35,
    },
    {
        "sku": "SKU-EL-007",
        "name": "Portable Phone Charger 20000mAh",
        "category": "Electronics",
        "price": 29.99,
        "unit_cost": 16.50,
        "reorder_point": 100,
        "safety_stock": 50,
    },
    {
        "sku": "SKU-EL-010",
        "name": "Noise-Cancelling Earbuds",
        "category": "Electronics",
        "price": 79.99,
        "unit_cost": 42.00,
        "reorder_point": 80,
        "safety_stock": 40,
    },
    {
        "sku": "SKU-EL-015",
        "name": "4K Webcam with Ring Light",
        "category": "Electronics",
        "price": 64.99,
        "unit_cost": 35.00,
        "reorder_point": 60,
        "safety_stock": 30,
    },
    {
        "sku": "SKU-AP-003",
        "name": "Cold Brew Coffee Concentrate",
        "category": "Food & Beverage",
        "price": 14.99,
        "unit_cost": 7.80,
        "reorder_point": 150,
        "safety_stock": 75,
    },
    {
        "sku": "SKU-AP-012",
        "name": "Organic Protein Bars (24pk)",
        "category": "Food & Beverage",
        "price": 32.50,
        "unit_cost": 18.75,
        "reorder_point": 200,
        "safety_stock": 100,
    },
    {
        "sku": "SKU-AP-018",
        "name": "Sparkling Water Variety Pack",
        "category": "Food & Beverage",
        "price": 22.99,
        "unit_cost": 12.50,
        "reorder_point": 250,
        "safety_stock": 125,
    },
    {
        "sku": "SKU-AP-025",
        "name": "Premium Trail Mix (12pk)",
        "category": "Food & Beverage",
        "price": 27.99,
        "unit_cost": 15.00,
        "reorder_point": 180,
        "safety_stock": 90,
    },
    {
        "sku": "SKU-HG-002",
        "name": "Stainless Steel Water Bottle",
        "category": "Home & Garden",
        "price": 22.50,
        "unit_cost": 12.30,
        "reorder_point": 150,
        "safety_stock": 75,
    },
    {
        "sku": "SKU-HG-005",
        "name": "Smart LED Bulb (4-pack)",
        "category": "Home & Garden",
        "price": 24.99,
        "unit_cost": 14.20,
        "reorder_point": 80,
        "safety_stock": 40,
    },
    {
        "sku": "SKU-HG-009",
        "name": "Bamboo Cutting Board Set",
        "category": "Home & Garden",
        "price": 34.99,
        "unit_cost": 18.50,
        "reorder_point": 60,
        "safety_stock": 30,
    },
    {
        "sku": "SKU-HG-014",
        "name": "Indoor Herb Garden Kit",
        "category": "Home & Garden",
        "price": 42.99,
        "unit_cost": 22.00,
        "reorder_point": 70,
        "safety_stock": 35,
    },
    {
        "sku": "SKU-HL-008",
        "name": "Vitamin D3 Supplements",
        "category": "Health & Wellness",
        "price": 18.99,
        "unit_cost": 8.50,
        "reorder_point": 250,
        "safety_stock": 120,
    },
    {
        "sku": "SKU-HL-015",
        "name": "Melatonin Sleep Gummies",
        "category": "Health & Wellness",
        "price": 12.99,
        "unit_cost": 6.20,
        "reorder_point": 120,
        "safety_stock": 60,
    },
    {
        "sku": "SKU-HL-022",
        "name": "Probiotic Capsules (60ct)",
        "category": "Health & Wellness",
        "price": 24.99,
        "unit_cost": 12.50,
        "reorder_point": 100,
        "safety_stock": 50,
    },
    {
        "sku": "SKU-HL-030",
        "name": "Collagen Powder (30 servings)",
        "category": "Health & Wellness",
        "price": 35.99,
        "unit_cost": 19.00,
        "reorder_point": 90,
        "safety_stock": 45,
    },
    {
        "sku": "SKU-CL-004",
        "name": "Moisture-Wicking Running Socks",
        "category": "Clothing & Apparel",
        "price": 8.99,
        "unit_cost": 4.10,
        "reorder_point": 400,
        "safety_stock": 200,
    },
    {
        "sku": "SKU-CL-011",
        "name": "Performance Compression Tights",
        "category": "Clothing & Apparel",
        "price": 44.99,
        "unit_cost": 24.00,
        "reorder_point": 120,
        "safety_stock": 60,
    },
    {
        "sku": "SKU-CL-019",
        "name": "Quick-Dry Hiking Shorts",
        "category": "Clothing & Apparel",
        "price": 38.99,
        "unit_cost": 20.00,
        "reorder_point": 100,
        "safety_stock": 50,
    },
]

WAREHOUSES = [
    {
        "warehouse_id": "WH-EAST-01",
        "warehouse_name": "Newark DC",
        "region": "Northeast",
        "lead_time_days": 5,
    },
    {
        "warehouse_id": "WH-EAST-02",
        "warehouse_name": "Atlanta DC",
        "region": "Southeast",
        "lead_time_days": 5,
    },
    {
        "warehouse_id": "WH-CENT-01",
        "warehouse_name": "Chicago DC",
        "region": "Midwest",
        "lead_time_days": 4,
    },
    {
        "warehouse_id": "WH-CENT-02",
        "warehouse_name": "Minneapolis DC",
        "region": "Great Lakes",
        "lead_time_days": 6,
    },
    {
        "warehouse_id": "WH-SOUTH-01",
        "warehouse_name": "Dallas DC",
        "region": "South Central",
        "lead_time_days": 5,
    },
    {
        "warehouse_id": "WH-SOUTH-02",
        "warehouse_name": "Miami DC",
        "region": "Southeast",
        "lead_time_days": 4,
    },
    {
        "warehouse_id": "WH-WEST-01",
        "warehouse_name": "Los Angeles DC",
        "region": "Pacific",
        "lead_time_days": 7,
    },
    {
        "warehouse_id": "WH-WEST-02",
        "warehouse_name": "Denver DC",
        "region": "Mountain",
        "lead_time_days": 6,
    },
]

FORECAST_REGIONS = sorted({warehouse["region"] for warehouse in WAREHOUSES})

TABLE_COLUMN_COMMENTS = {
    SHIPMENT_TABLE: {
        "order_id": "Unique order identifier",
        "order_date": "Date order was placed",
        "customer_id": "Unique customer identifier",
        "product_sku": "Product SKU code",
        "product_name": "Human-readable product name",
        "product_category": (
            "Product category: Electronics, Food & Beverage, Home & Garden, "
            "Health & Wellness, Clothing & Apparel"
        ),
        "quantity": "Number of units ordered",
        "unit_price": "Price per unit in USD",
        "warehouse_id": "Originating distribution center identifier",
        "destination_region": (
            "Geographic delivery region: Northeast, Southeast, Midwest, Great "
            "Lakes, South Central, Pacific, Mountain"
        ),
        "delivery_date": "Actual or expected delivery date",
        "order_status": "Order status: Delivered, Backorder, In Transit, or Cancelled",
    },
    INVENTORY_TABLE: {
        "product_sku": "Product SKU code",
        "product_name": "Human-readable product name",
        "product_category": "Product category grouping",
        "warehouse_id": "Distribution center identifier",
        "warehouse_name": (
            "Warehouse name: Newark DC, Atlanta DC, Chicago DC, Minneapolis DC, "
            "Dallas DC, Miami DC, Los Angeles DC, Denver DC"
        ),
        "snapshot_date": "Date of weekly inventory snapshot",
        "quantity_on_hand": "Current quantity available in warehouse",
        "reorder_point": "Stock level that triggers a purchase order",
        "safety_stock_qty": "Minimum safety stock buffer to prevent stockouts",
        "lead_time_days": "Supplier lead time in days for replenishment",
        "unit_cost": "Cost per unit in USD",
    },
    FORECAST_TABLE: {
        "product_sku": "Product SKU code",
        "product_name": "Human-readable product name",
        "product_category": "Product category grouping",
        "forecast_date": "First day of the month this forecast applies to",
        "predicted_demand": "Predicted monthly demand in units",
        "confidence_lower": "Lower bound of the forecast confidence interval",
        "confidence_upper": "Upper bound of the forecast confidence interval",
        "actual_demand": "Actual observed demand in units",
        "forecast_error_pct": "Absolute forecast error as a percentage of actual demand",
        "model_version": "Forecast model version: v3.0, v3.1, or v3.2",
        "region": (
            "Geographic region: Northeast, Southeast, Midwest, Great Lakes, "
            "South Central, Pacific, Mountain"
        ),
    },
}


def table_fqdns(fqn: str) -> dict[str, str]:
    """Return fully qualified names for all managed tables."""

    return {
        SHIPMENT_TABLE: f"{fqn}.{SHIPMENT_TABLE}",
        INVENTORY_TABLE: f"{fqn}.{INVENTORY_TABLE}",
        FORECAST_TABLE: f"{fqn}.{FORECAST_TABLE}",
    }


def build_table_sqls(fqn: str, seed: int) -> dict[str, str]:
    """Build all deterministic CTAS statements."""

    return {
        SHIPMENT_TABLE: build_shipment_orders_sql(fqn, seed),
        INVENTORY_TABLE: build_inventory_levels_sql(fqn, seed),
        FORECAST_TABLE: build_demand_forecasts_sql(fqn, seed),
    }


def build_shipment_orders_sql(fqn: str, seed: int) -> str:
    """Build the deterministic shipment orders table."""

    products_values = _values_sql(
        [[p["sku"], p["name"], p["category"], p["price"]] for p in PRODUCTS]
    )
    warehouse_values = _values_sql(
        [
            [w["warehouse_id"], w["warehouse_name"], w["region"]]
            for w in WAREHOUSES
        ]
    )

    customer_idx = _hash_int(seed, "customer_idx", "d.order_date", "p.sku", "w.wh_id", modulo=500)
    qty_noise = _hash_fraction(seed, "qty_noise", "d.order_date", "p.sku", "w.wh_id")
    status_noise = _hash_fraction(seed, "status_noise", "d.order_date", "p.sku", "w.wh_id")
    select_noise = _hash_fraction(seed, "select_noise", "d.order_date", "p.sku", "w.wh_id")
    delivery_offset = _hash_int(
        seed,
        "delivery_offset",
        "d.order_date",
        "p.sku",
        "w.wh_id",
        modulo=4,
        offset=2,
    )

    return f"""
CREATE OR REPLACE TABLE {fqn}.{SHIPMENT_TABLE} AS
WITH
products AS (
  SELECT * FROM VALUES
{products_values}
  AS t(sku, name, category, price)
),
warehouses AS (
  SELECT * FROM VALUES
{warehouse_values}
  AS t(wh_id, wh_name, region)
),
date_range AS (
  SELECT EXPLODE(SEQUENCE(DATE'2025-01-01', DATE'2025-12-31', INTERVAL 1 DAY)) AS order_date
),
order_skeleton AS (
  SELECT
    d.order_date,
    p.sku,
    p.name,
    p.category,
    p.price,
    w.wh_id,
    w.region,
    {customer_idx} AS customer_idx,
    {qty_noise} AS qty_noise,
    {status_noise} AS status_noise,
    {select_noise} AS select_noise,
    {delivery_offset} AS delivery_offset,
    MONTH(d.order_date) AS mo,
    DAYOFWEEK(d.order_date) AS dow
  FROM date_range d
  CROSS JOIN products p
  CROSS JOIN warehouses w
),
filtered AS (
  SELECT *,
    CASE
      WHEN category = 'Electronics' AND mo IN (11, 12) THEN 0.0600
      WHEN category = 'Electronics' THEN 0.0250
      WHEN category = 'Clothing & Apparel' AND mo IN (3, 4, 9, 10) THEN 0.0550
      WHEN category = 'Clothing & Apparel' THEN 0.0200
      WHEN category = 'Food & Beverage' THEN 0.0350
      WHEN category = 'Health & Wellness' AND mo IN (1, 2) THEN 0.0500
      WHEN category = 'Health & Wellness' THEN 0.0300
      WHEN category = 'Home & Garden' AND mo IN (4, 5, 6) THEN 0.0500
      WHEN category = 'Home & Garden' THEN 0.0250
      ELSE 0.0300
    END AS selection_prob
  FROM order_skeleton
  WHERE dow BETWEEN 2 AND 6
)
SELECT
  CONCAT(
    'ORD-',
    LPAD(CAST(ROW_NUMBER() OVER (ORDER BY order_date, sku, wh_id) + 10000 AS STRING), 6, '0')
  ) AS order_id,
  order_date,
  CONCAT('CUST-', LPAD(CAST(1001 + customer_idx AS STRING), 4, '0')) AS customer_id,
  sku AS product_sku,
  name AS product_name,
  category AS product_category,
  CASE
    WHEN category = 'Clothing & Apparel' THEN CAST(50 + FLOOR(qty_noise * 450) AS INT)
    WHEN category = 'Food & Beverage' THEN CAST(30 + FLOOR(qty_noise * 250) AS INT)
    WHEN category = 'Health & Wellness' THEN CAST(20 + FLOOR(qty_noise * 280) AS INT)
    WHEN category = 'Home & Garden' THEN CAST(10 + FLOOR(qty_noise * 150) AS INT)
    WHEN category = 'Electronics' THEN CAST(5 + FLOOR(qty_noise * 80) AS INT)
    ELSE CAST(10 + FLOOR(qty_noise * 100) AS INT)
  END AS quantity,
  price AS unit_price,
  wh_id AS warehouse_id,
  region AS destination_region,
  DATE_ADD(order_date, delivery_offset) AS delivery_date,
  CASE
    WHEN status_noise < 0.0200 THEN 'Cancelled'
    WHEN status_noise < 0.0500 THEN 'Backorder'
    WHEN status_noise < 0.0800 AND order_date > DATE'2025-11-15' THEN 'In Transit'
    ELSE 'Delivered'
  END AS order_status
FROM filtered
WHERE select_noise < selection_prob
""".strip()


def build_inventory_levels_sql(fqn: str, seed: int) -> str:
    """Build the deterministic weekly inventory snapshot table."""

    products_values = _values_sql(
        [
            [
                p["sku"],
                p["name"],
                p["category"],
                p["unit_cost"],
                p["reorder_point"],
                p["safety_stock"],
            ]
            for p in PRODUCTS
        ]
    )
    warehouse_values = _values_sql(
        [
            [w["warehouse_id"], w["warehouse_name"], w["lead_time_days"]]
            for w in WAREHOUSES
        ]
    )

    pair_hash = _hash_int(seed, "inventory_pair", "p.sku", "w.wh_id", modulo=100)
    seasonal_noise = _hash_fraction(
        seed,
        "inventory_noise",
        "d.snapshot_date",
        "a.sku",
        "a.wh_id",
    )
    lead_delta = _hash_int(
        seed,
        "lead_delta",
        "d.snapshot_date",
        "a.sku",
        "a.wh_id",
        modulo=3,
        offset=-1,
    )

    return f"""
CREATE OR REPLACE TABLE {fqn}.{INVENTORY_TABLE} AS
WITH
products AS (
  SELECT * FROM VALUES
{products_values}
  AS t(sku, name, category, unit_cost, reorder_pt, safety_stock)
),
warehouses AS (
  SELECT * FROM VALUES
{warehouse_values}
  AS t(wh_id, wh_name, lead_time)
),
snapshot_dates AS (
  SELECT EXPLODE(SEQUENCE(DATE'2025-01-01', DATE'2025-12-31', INTERVAL 7 DAY)) AS snapshot_date
),
product_warehouse AS (
  SELECT
    p.*,
    w.wh_id,
    w.wh_name,
    w.lead_time,
    {pair_hash} AS pair_hash
  FROM products p
  CROSS JOIN warehouses w
),
assigned AS (
  SELECT *
  FROM product_warehouse
  WHERE pair_hash < 52
)
SELECT
  a.sku AS product_sku,
  a.name AS product_name,
  a.category AS product_category,
  a.wh_id AS warehouse_id,
  a.wh_name AS warehouse_name,
  d.snapshot_date,
  GREATEST(
    0,
    CAST(
      a.reorder_pt * 1.8
      + (a.reorder_pt * 0.6 * SIN(DAYOFYEAR(d.snapshot_date) * 3.14159 / 180.0 + pmod(hash(a.sku), 100)))
      + (a.reorder_pt * 0.3 * ({seasonal_noise} - 0.5))
      - CASE
          WHEN a.category = 'Electronics' AND MONTH(d.snapshot_date) IN (11, 12) THEN a.reorder_pt * 0.5
          WHEN a.category = 'Clothing & Apparel' AND MONTH(d.snapshot_date) IN (3, 4, 9) THEN a.reorder_pt * 0.3
          WHEN a.category = 'Health & Wellness' AND MONTH(d.snapshot_date) IN (1, 2) THEN a.reorder_pt * 0.4
          WHEN a.category = 'Home & Garden' AND MONTH(d.snapshot_date) IN (5, 6) THEN a.reorder_pt * 0.3
          ELSE 0
        END
      AS INT
    )
  ) AS quantity_on_hand,
  a.reorder_pt AS reorder_point,
  a.safety_stock AS safety_stock_qty,
  a.lead_time + {lead_delta} AS lead_time_days,
  a.unit_cost
FROM assigned a
CROSS JOIN snapshot_dates d
""".strip()


def build_demand_forecasts_sql(fqn: str, seed: int) -> str:
    """Build the deterministic monthly forecast table."""

    products_values = _values_sql(
        [[p["sku"], p["name"], p["category"]] for p in PRODUCTS]
    )
    region_values = _values_sql([[region] for region in FORECAST_REGIONS])

    pair_hash = _hash_int(seed, "forecast_pair", "p.sku", "r.region", modulo=100)
    forecast_noise = _hash_fraction(
        seed,
        "forecast_noise",
        "m.forecast_date",
        "p.sku",
        "r.region",
    )
    lower_noise = _hash_fraction(
        seed,
        "lower_noise",
        "forecast_date",
        "product_sku",
        "region",
    )
    upper_noise = _hash_fraction(
        seed,
        "upper_noise",
        "forecast_date",
        "product_sku",
        "region",
    )
    actual_noise = _hash_fraction(
        seed,
        "actual_noise",
        "forecast_date",
        "product_sku",
        "region",
    )

    return f"""
CREATE OR REPLACE TABLE {fqn}.{FORECAST_TABLE} AS
WITH
products AS (
  SELECT * FROM VALUES
{products_values}
  AS t(sku, name, category)
),
regions AS (
  SELECT * FROM VALUES
{region_values}
  AS t(region)
),
months AS (
  SELECT EXPLODE(SEQUENCE(DATE'2025-01-01', DATE'2025-12-01', INTERVAL 1 MONTH)) AS forecast_date
),
base AS (
  SELECT
    p.sku,
    p.name,
    p.category,
    r.region,
    m.forecast_date,
    CASE
      WHEN m.forecast_date < DATE'2025-05-01' THEN 'v3.0'
      WHEN m.forecast_date < DATE'2025-09-01' THEN 'v3.1'
      ELSE 'v3.2'
    END AS model_version,
    CASE
      WHEN p.category = 'Electronics' THEN 200
      WHEN p.category = 'Food & Beverage' THEN 350
      WHEN p.category = 'Health & Wellness' THEN 300
      WHEN p.category = 'Home & Garden' THEN 150
      WHEN p.category = 'Clothing & Apparel' THEN 250
      ELSE 200
    END AS base_demand,
    {pair_hash} AS pair_hash,
    {forecast_noise} AS forecast_noise
  FROM products p
  CROSS JOIN regions r
  CROSS JOIN months m
),
forecasts AS (
  SELECT
    sku AS product_sku,
    name AS product_name,
    category AS product_category,
    forecast_date,
    model_version,
    region,
    CAST(
      base_demand
      * (1.0 + 0.03 * MONTH(forecast_date))
      * CASE
          WHEN category = 'Electronics' AND MONTH(forecast_date) IN (11, 12) THEN 1.8
          WHEN category = 'Electronics' AND MONTH(forecast_date) IN (6, 7) THEN 0.7
          WHEN category = 'Clothing & Apparel' AND MONTH(forecast_date) IN (3, 4) THEN 1.5
          WHEN category = 'Clothing & Apparel' AND MONTH(forecast_date) IN (9, 10) THEN 1.4
          WHEN category = 'Food & Beverage' AND MONTH(forecast_date) IN (5, 6, 7) THEN 1.3
          WHEN category = 'Health & Wellness' AND MONTH(forecast_date) IN (1, 2) THEN 1.6
          WHEN category = 'Home & Garden' AND MONTH(forecast_date) IN (4, 5, 6) THEN 1.5
          ELSE 1.0
        END
      * (0.85 + 0.30 * forecast_noise)
      AS INT
    ) AS predicted_demand
  FROM base
  WHERE pair_hash < 52
),
actualized AS (
  SELECT
    product_sku,
    product_name,
    product_category,
    forecast_date,
    predicted_demand,
    CAST(predicted_demand * (0.82 + 0.03 * {lower_noise}) AS INT) AS confidence_lower,
    CAST(predicted_demand * (1.12 + 0.05 * {upper_noise}) AS INT) AS confidence_upper,
    CAST(predicted_demand * (0.88 + 0.24 * {actual_noise}) AS INT) AS actual_demand,
    model_version,
    region
  FROM forecasts
)
SELECT
  product_sku,
  product_name,
  product_category,
  forecast_date,
  predicted_demand,
  confidence_lower,
  confidence_upper,
  actual_demand,
  ROUND(ABS(actual_demand - predicted_demand) / NULLIF(actual_demand, 0) * 100, 1) AS forecast_error_pct,
  model_version,
  region
FROM actualized
""".strip()


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
