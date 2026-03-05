# Databricks notebook source

# MAGIC %md
# MAGIC # NorthStar Logistics — Demand Forecasting & Inventory Management
# MAGIC
# MAGIC This notebook provisions a complete Genie data room for a logistics demand forecasting scenario:
# MAGIC
# MAGIC 1. Creates a Unity Catalog **catalog** and **schema**
# MAGIC 2. Generates three large-scale synthetic tables (~6,700 rows total)
# MAGIC 3. Deploys a fully-configured **Genie space** with instructions, example SQL, and benchmarks
# MAGIC
# MAGIC **Usage:** Fill in the widgets at the top, then **Run All**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Configuration

# COMMAND ----------

import re

current_user = spark.sql("SELECT current_user()").first()[0]
default_catalog = re.sub(r"[^a-zA-Z0-9]", "_", current_user.split("@")[0]).lower()

dbutils.widgets.text("catalog_name", default_catalog, "Catalog Name")
dbutils.widgets.text("schema_name", "demand_forecasting", "Schema Name")
dbutils.widgets.text("warehouse_id", "", "SQL Warehouse ID (for Genie space)")

CATALOG = dbutils.widgets.get("catalog_name")
SCHEMA  = dbutils.widgets.get("schema_name")
WH_ID   = dbutils.widgets.get("warehouse_id")
FQN     = f"{CATALOG}.{SCHEMA}"

print(f"Catalog : {CATALOG}")
print(f"Schema  : {FQN}")
print(f"Warehouse: {WH_ID or '(will skip Genie space creation)'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Create Catalog & Schema

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FQN} COMMENT 'Logistics demand forecasting and inventory management – Genie demo data'")
print(f"Schema {FQN} ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Generate `shipment_orders`
# MAGIC
# MAGIC ~1,300+ order transactions across 20 SKUs, 8 warehouses, 12 months with seasonal demand patterns.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {FQN}.shipment_orders AS
WITH
products AS (
  SELECT * FROM VALUES
    ('SKU-EL-001', 'Wireless Bluetooth Speaker', 'Electronics', 49.99),
    ('SKU-EL-003', 'USB-C Hub 7-in-1', 'Electronics', 39.99),
    ('SKU-EL-007', 'Portable Phone Charger 20000mAh', 'Electronics', 29.99),
    ('SKU-EL-010', 'Noise-Cancelling Earbuds', 'Electronics', 79.99),
    ('SKU-EL-015', '4K Webcam with Ring Light', 'Electronics', 64.99),
    ('SKU-AP-003', 'Cold Brew Coffee Concentrate', 'Food & Beverage', 14.99),
    ('SKU-AP-012', 'Organic Protein Bars (24pk)', 'Food & Beverage', 32.50),
    ('SKU-AP-018', 'Sparkling Water Variety Pack', 'Food & Beverage', 22.99),
    ('SKU-AP-025', 'Premium Trail Mix (12pk)', 'Food & Beverage', 27.99),
    ('SKU-HG-002', 'Stainless Steel Water Bottle', 'Home & Garden', 22.50),
    ('SKU-HG-005', 'Smart LED Bulb (4-pack)', 'Home & Garden', 24.99),
    ('SKU-HG-009', 'Bamboo Cutting Board Set', 'Home & Garden', 34.99),
    ('SKU-HG-014', 'Indoor Herb Garden Kit', 'Home & Garden', 42.99),
    ('SKU-HL-008', 'Vitamin D3 Supplements', 'Health & Wellness', 18.99),
    ('SKU-HL-015', 'Melatonin Sleep Gummies', 'Health & Wellness', 12.99),
    ('SKU-HL-022', 'Probiotic Capsules (60ct)', 'Health & Wellness', 24.99),
    ('SKU-HL-030', 'Collagen Powder (30 servings)', 'Health & Wellness', 35.99),
    ('SKU-CL-004', 'Moisture-Wicking Running Socks', 'Clothing & Apparel', 8.99),
    ('SKU-CL-011', 'Performance Compression Tights', 'Clothing & Apparel', 44.99),
    ('SKU-CL-019', 'Quick-Dry Hiking Shorts', 'Clothing & Apparel', 38.99)
  AS t(sku, name, category, price)
),
warehouses AS (
  SELECT * FROM VALUES
    ('WH-EAST-01', 'Newark DC', 'Northeast'),
    ('WH-EAST-02', 'Atlanta DC', 'Southeast'),
    ('WH-CENT-01', 'Chicago DC', 'Midwest'),
    ('WH-CENT-02', 'Minneapolis DC', 'Great Lakes'),
    ('WH-SOUTH-01', 'Dallas DC', 'South Central'),
    ('WH-SOUTH-02', 'Miami DC', 'Southeast'),
    ('WH-WEST-01', 'Los Angeles DC', 'Pacific'),
    ('WH-WEST-02', 'Denver DC', 'Mountain')
  AS t(wh_id, wh_name, region)
),
date_range AS (
  SELECT EXPLODE(SEQUENCE(DATE'2025-01-01', DATE'2025-12-31', INTERVAL 1 DAY)) AS order_date
),
order_skeleton AS (
  SELECT
    d.order_date,
    p.sku, p.name, p.category, p.price,
    w.wh_id, w.region,
    FLOOR(RAND() * 500) AS rand_cust,
    RAND() AS rand_qty,
    RAND() AS rand_status,
    RAND() AS rand_select,
    MONTH(d.order_date) AS mo,
    DAYOFWEEK(d.order_date) AS dow
  FROM date_range d
  CROSS JOIN products p
  CROSS JOIN warehouses w
),
filtered AS (
  SELECT *,
    CASE
      WHEN category = 'Electronics' AND mo IN (11, 12) THEN 0.06
      WHEN category = 'Electronics' THEN 0.025
      WHEN category = 'Clothing & Apparel' AND mo IN (3, 4, 9, 10) THEN 0.055
      WHEN category = 'Clothing & Apparel' THEN 0.02
      WHEN category = 'Food & Beverage' THEN 0.035
      WHEN category = 'Health & Wellness' AND mo IN (1, 2) THEN 0.05
      WHEN category = 'Health & Wellness' THEN 0.03
      WHEN category = 'Home & Garden' AND mo IN (4, 5, 6) THEN 0.05
      WHEN category = 'Home & Garden' THEN 0.025
      ELSE 0.03
    END AS selection_prob
  FROM order_skeleton
  WHERE dow BETWEEN 2 AND 6
)
SELECT
  CONCAT('ORD-', LPAD(CAST(ROW_NUMBER() OVER (ORDER BY order_date, sku, wh_id) + 10000 AS STRING), 6, '0')) AS order_id,
  order_date,
  CONCAT('CUST-', LPAD(CAST(1001 + CAST(rand_cust AS INT) AS STRING), 4, '0')) AS customer_id,
  sku AS product_sku,
  name AS product_name,
  category AS product_category,
  CASE
    WHEN category = 'Clothing & Apparel' THEN CAST(50 + FLOOR(rand_qty * 450) AS INT)
    WHEN category = 'Food & Beverage' THEN CAST(30 + FLOOR(rand_qty * 250) AS INT)
    WHEN category = 'Health & Wellness' THEN CAST(20 + FLOOR(rand_qty * 280) AS INT)
    WHEN category = 'Home & Garden' THEN CAST(10 + FLOOR(rand_qty * 150) AS INT)
    WHEN category = 'Electronics' THEN CAST(5 + FLOOR(rand_qty * 80) AS INT)
    ELSE CAST(10 + FLOOR(rand_qty * 100) AS INT)
  END AS quantity,
  price AS unit_price,
  wh_id AS warehouse_id,
  region AS destination_region,
  DATE_ADD(order_date, CAST(2 + FLOOR(RAND() * 4) AS INT)) AS delivery_date,
  CASE
    WHEN rand_status < 0.02 THEN 'Cancelled'
    WHEN rand_status < 0.05 THEN 'Backorder'
    WHEN rand_status < 0.08 AND order_date > DATE'2025-11-15' THEN 'In Transit'
    ELSE 'Delivered'
  END AS order_status
FROM filtered
WHERE rand_select < selection_prob
""")

print(f"shipment_orders: {spark.table(f'{FQN}.shipment_orders').count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Generate `inventory_levels`
# MAGIC
# MAGIC ~4,400+ weekly inventory snapshots across 20 SKUs, 8 warehouses, 53 weeks with seasonal depletion.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {FQN}.inventory_levels AS
WITH
products AS (
  SELECT * FROM VALUES
    ('SKU-EL-001', 'Wireless Bluetooth Speaker', 'Electronics', 28.50, 100, 50),
    ('SKU-EL-003', 'USB-C Hub 7-in-1', 'Electronics', 22.00, 75, 35),
    ('SKU-EL-007', 'Portable Phone Charger 20000mAh', 'Electronics', 16.50, 100, 50),
    ('SKU-EL-010', 'Noise-Cancelling Earbuds', 'Electronics', 42.00, 80, 40),
    ('SKU-EL-015', '4K Webcam with Ring Light', 'Electronics', 35.00, 60, 30),
    ('SKU-AP-003', 'Cold Brew Coffee Concentrate', 'Food & Beverage', 7.80, 150, 75),
    ('SKU-AP-012', 'Organic Protein Bars (24pk)', 'Food & Beverage', 18.75, 200, 100),
    ('SKU-AP-018', 'Sparkling Water Variety Pack', 'Food & Beverage', 12.50, 250, 125),
    ('SKU-AP-025', 'Premium Trail Mix (12pk)', 'Food & Beverage', 15.00, 180, 90),
    ('SKU-HG-002', 'Stainless Steel Water Bottle', 'Home & Garden', 12.30, 150, 75),
    ('SKU-HG-005', 'Smart LED Bulb (4-pack)', 'Home & Garden', 14.20, 80, 40),
    ('SKU-HG-009', 'Bamboo Cutting Board Set', 'Home & Garden', 18.50, 60, 30),
    ('SKU-HG-014', 'Indoor Herb Garden Kit', 'Home & Garden', 22.00, 70, 35),
    ('SKU-HL-008', 'Vitamin D3 Supplements', 'Health & Wellness', 8.50, 250, 120),
    ('SKU-HL-015', 'Melatonin Sleep Gummies', 'Health & Wellness', 6.20, 120, 60),
    ('SKU-HL-022', 'Probiotic Capsules (60ct)', 'Health & Wellness', 12.50, 100, 50),
    ('SKU-HL-030', 'Collagen Powder (30 servings)', 'Health & Wellness', 19.00, 90, 45),
    ('SKU-CL-004', 'Moisture-Wicking Running Socks', 'Clothing & Apparel', 4.10, 400, 200),
    ('SKU-CL-011', 'Performance Compression Tights', 'Clothing & Apparel', 24.00, 120, 60),
    ('SKU-CL-019', 'Quick-Dry Hiking Shorts', 'Clothing & Apparel', 20.00, 100, 50)
  AS t(sku, name, category, unit_cost, reorder_pt, safety_stock)
),
warehouses AS (
  SELECT * FROM VALUES
    ('WH-EAST-01', 'Newark DC', 5),
    ('WH-EAST-02', 'Atlanta DC', 5),
    ('WH-CENT-01', 'Chicago DC', 4),
    ('WH-CENT-02', 'Minneapolis DC', 6),
    ('WH-SOUTH-01', 'Dallas DC', 5),
    ('WH-SOUTH-02', 'Miami DC', 4),
    ('WH-WEST-01', 'Los Angeles DC', 7),
    ('WH-WEST-02', 'Denver DC', 6)
  AS t(wh_id, wh_name, lead_time)
),
snapshot_dates AS (
  SELECT EXPLODE(SEQUENCE(DATE'2025-01-01', DATE'2025-12-31', INTERVAL 7 DAY)) AS snapshot_date
),
product_warehouse AS (
  SELECT p.*, w.wh_id, w.wh_name, w.lead_time,
    ABS(HASH(CONCAT(p.sku, w.wh_id))) % 100 AS pair_hash
  FROM products p CROSS JOIN warehouses w
),
assigned AS (
  SELECT * FROM product_warehouse
  WHERE pair_hash < 45
)
SELECT
  a.sku AS product_sku,
  a.name AS product_name,
  a.category AS product_category,
  a.wh_id AS warehouse_id,
  a.wh_name AS warehouse_name,
  d.snapshot_date,
  GREATEST(0, CAST(
    a.reorder_pt * 1.8
    + (a.reorder_pt * 0.6 * SIN(DAYOFYEAR(d.snapshot_date) * 3.14159 / 180.0 + ABS(HASH(a.sku)) % 100))
    + (a.reorder_pt * 0.3 * (RAND() - 0.5))
    - CASE
        WHEN a.category = 'Electronics' AND MONTH(d.snapshot_date) IN (11, 12) THEN a.reorder_pt * 0.5
        WHEN a.category = 'Clothing & Apparel' AND MONTH(d.snapshot_date) IN (3, 4, 9) THEN a.reorder_pt * 0.3
        WHEN a.category = 'Health & Wellness' AND MONTH(d.snapshot_date) IN (1, 2) THEN a.reorder_pt * 0.4
        WHEN a.category = 'Home & Garden' AND MONTH(d.snapshot_date) IN (5, 6) THEN a.reorder_pt * 0.3
        ELSE 0
      END
  AS INT)) AS quantity_on_hand,
  a.reorder_pt AS reorder_point,
  a.safety_stock AS safety_stock_qty,
  a.lead_time + CAST(FLOOR(RAND() * 3) - 1 AS INT) AS lead_time_days,
  a.unit_cost
FROM assigned a
CROSS JOIN snapshot_dates d
""")

print(f"inventory_levels: {spark.table(f'{FQN}.inventory_levels').count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Generate `demand_forecasts`
# MAGIC
# MAGIC ~870+ monthly ML forecasts across 20 SKUs, 6 regions, 3 model versions with improving accuracy.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {FQN}.demand_forecasts AS
WITH
products AS (
  SELECT * FROM VALUES
    ('SKU-EL-001', 'Wireless Bluetooth Speaker', 'Electronics'),
    ('SKU-EL-003', 'USB-C Hub 7-in-1', 'Electronics'),
    ('SKU-EL-007', 'Portable Phone Charger 20000mAh', 'Electronics'),
    ('SKU-EL-010', 'Noise-Cancelling Earbuds', 'Electronics'),
    ('SKU-EL-015', '4K Webcam with Ring Light', 'Electronics'),
    ('SKU-AP-003', 'Cold Brew Coffee Concentrate', 'Food & Beverage'),
    ('SKU-AP-012', 'Organic Protein Bars (24pk)', 'Food & Beverage'),
    ('SKU-AP-018', 'Sparkling Water Variety Pack', 'Food & Beverage'),
    ('SKU-AP-025', 'Premium Trail Mix (12pk)', 'Food & Beverage'),
    ('SKU-HG-002', 'Stainless Steel Water Bottle', 'Home & Garden'),
    ('SKU-HG-005', 'Smart LED Bulb (4-pack)', 'Home & Garden'),
    ('SKU-HG-009', 'Bamboo Cutting Board Set', 'Home & Garden'),
    ('SKU-HG-014', 'Indoor Herb Garden Kit', 'Home & Garden'),
    ('SKU-HL-008', 'Vitamin D3 Supplements', 'Health & Wellness'),
    ('SKU-HL-015', 'Melatonin Sleep Gummies', 'Health & Wellness'),
    ('SKU-HL-022', 'Probiotic Capsules (60ct)', 'Health & Wellness'),
    ('SKU-HL-030', 'Collagen Powder (30 servings)', 'Health & Wellness'),
    ('SKU-CL-004', 'Moisture-Wicking Running Socks', 'Clothing & Apparel'),
    ('SKU-CL-011', 'Performance Compression Tights', 'Clothing & Apparel'),
    ('SKU-CL-019', 'Quick-Dry Hiking Shorts', 'Clothing & Apparel')
  AS t(sku, name, category)
),
regions AS (
  SELECT * FROM VALUES
    ('Northeast'), ('Southeast'), ('Midwest'),
    ('Great Lakes'), ('South Central'), ('Pacific')
  AS t(region)
),
months AS (
  SELECT EXPLODE(SEQUENCE(DATE'2025-01-01', DATE'2025-12-01', INTERVAL 1 MONTH)) AS forecast_date
),
base AS (
  SELECT
    p.sku, p.name, p.category,
    r.region, m.forecast_date,
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
    ABS(HASH(CONCAT(p.sku, r.region))) % 100 AS pair_hash
  FROM products p CROSS JOIN regions r CROSS JOIN months m
),
forecasts AS (
  SELECT
    sku AS product_sku, name AS product_name, category AS product_category,
    forecast_date, model_version, region,
    CAST(base_demand
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
      * (0.85 + 0.3 * RAND())
    AS INT) AS predicted_demand
  FROM base
  WHERE pair_hash < 60
)
SELECT
  product_sku, product_name, product_category, forecast_date,
  predicted_demand,
  CAST(predicted_demand * (0.82 + 0.03 * RAND()) AS INT) AS confidence_lower,
  CAST(predicted_demand * (1.12 + 0.05 * RAND()) AS INT) AS confidence_upper,
  CAST(predicted_demand * (0.88 + 0.24 * RAND()) AS INT) AS actual_demand,
  ROUND(ABS(
    (CAST(predicted_demand * (0.88 + 0.24 * RAND()) AS INT) - predicted_demand)
    / NULLIF(CAST(predicted_demand * (0.88 + 0.24 * RAND()) AS INT), 0) * 100
  ), 1) AS forecast_error_pct,
  model_version, region
FROM forecasts
""")

print(f"demand_forecasts: {spark.table(f'{FQN}.demand_forecasts').count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Add Column Comments

# COMMAND ----------

column_comments = {
    f"{FQN}.shipment_orders": {
        "order_id": "Unique order identifier",
        "order_date": "Date order was placed",
        "customer_id": "Unique customer identifier",
        "product_sku": "Product SKU code",
        "product_name": "Human-readable product name",
        "product_category": "Product category: Electronics, Food & Beverage, Home & Garden, Health & Wellness, Clothing & Apparel",
        "quantity": "Number of units ordered",
        "unit_price": "Price per unit in USD",
        "warehouse_id": "Originating distribution center identifier",
        "destination_region": "Geographic delivery region: Northeast, Southeast, Midwest, Great Lakes, South Central, Pacific, Mountain",
        "delivery_date": "Actual or expected delivery date",
        "order_status": "Order status: Delivered, Backorder, In Transit, or Cancelled",
    },
    f"{FQN}.inventory_levels": {
        "product_sku": "Product SKU code",
        "product_name": "Human-readable product name",
        "product_category": "Product category grouping",
        "warehouse_id": "Distribution center identifier",
        "warehouse_name": "Warehouse name: Newark DC, Atlanta DC, Chicago DC, Minneapolis DC, Dallas DC, Miami DC, Los Angeles DC, Denver DC",
        "snapshot_date": "Date of weekly inventory snapshot",
        "quantity_on_hand": "Current quantity available in warehouse",
        "reorder_point": "Stock level that triggers a purchase order",
        "safety_stock_qty": "Minimum safety stock buffer to prevent stockouts",
        "lead_time_days": "Supplier lead time in days for replenishment",
        "unit_cost": "Cost per unit in USD",
    },
    f"{FQN}.demand_forecasts": {
        "product_sku": "Product SKU code",
        "product_name": "Human-readable product name",
        "product_category": "Product category grouping",
        "forecast_date": "First day of the month this forecast applies to",
        "predicted_demand": "ML model predicted demand in units for the month",
        "confidence_lower": "Lower bound of 95pct confidence interval",
        "confidence_upper": "Upper bound of 95pct confidence interval",
        "actual_demand": "Actual observed demand in units",
        "forecast_error_pct": "Absolute forecast error as percentage of actual demand",
        "model_version": "ML model version: v3.0, v3.1, or v3.2 (improving over time)",
        "region": "Geographic region: Northeast, Southeast, Midwest, Great Lakes, South Central, Pacific",
    },
}

for table, cols in column_comments.items():
    for col, comment in cols.items():
        spark.sql(f"ALTER TABLE {table} ALTER COLUMN {col} COMMENT '{comment}'")

print("Column comments applied to all 3 tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Verify Data

# COMMAND ----------

display(spark.sql(f"""
  SELECT 'shipment_orders' AS table_name, COUNT(*) AS row_count FROM {FQN}.shipment_orders
  UNION ALL
  SELECT 'inventory_levels', COUNT(*) FROM {FQN}.inventory_levels
  UNION ALL
  SELECT 'demand_forecasts', COUNT(*) FROM {FQN}.demand_forecasts
"""))

# COMMAND ----------

display(spark.sql(f"""
  SELECT product_category,
    COUNT(*) AS orders,
    SUM(quantity) AS total_units,
    ROUND(SUM(quantity * unit_price), 2) AS total_revenue,
    COUNT(DISTINCT customer_id) AS unique_customers,
    MIN(order_date) AS first_order,
    MAX(order_date) AS last_order
  FROM {FQN}.shipment_orders
  GROUP BY product_category
  ORDER BY total_revenue DESC
"""))

# COMMAND ----------

display(spark.sql(f"""
  SELECT product_category,
    COUNT(*) AS snapshots,
    COUNT(DISTINCT warehouse_id) AS warehouses,
    COUNT(DISTINCT snapshot_date) AS weeks,
    ROUND(AVG(quantity_on_hand)) AS avg_qty_on_hand,
    SUM(CASE WHEN quantity_on_hand <= reorder_point THEN 1 ELSE 0 END) AS below_reorder_point,
    SUM(CASE WHEN quantity_on_hand < safety_stock_qty THEN 1 ELSE 0 END) AS below_safety_stock
  FROM {FQN}.inventory_levels
  GROUP BY product_category ORDER BY product_category
"""))

# COMMAND ----------

display(spark.sql(f"""
  SELECT product_category, model_version,
    COUNT(*) AS forecasts,
    ROUND(AVG(predicted_demand)) AS avg_predicted,
    ROUND(AVG(actual_demand)) AS avg_actual,
    ROUND(AVG(forecast_error_pct), 1) AS avg_error_pct,
    ROUND(AVG(100 - forecast_error_pct), 1) AS avg_accuracy_pct
  FROM {FQN}.demand_forecasts
  GROUP BY product_category, model_version
  ORDER BY product_category, model_version
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 · Create Genie Space

# COMMAND ----------

import json, requests

if not WH_ID:
    print("⚠ No warehouse_id provided — skipping Genie space creation.")
    print("  Re-run with a warehouse_id widget value to create the Genie space.")
    dbutils.notebook.exit("Tables created. Genie space skipped (no warehouse_id).")

workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
username = spark.sql("SELECT current_user()").first()[0]

headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
api_base = f"https://{workspace_url}"

serialized_space = {
    "version": 2,
    "config": {
        "sample_questions": [
            {"id": "a1b2c3d4e5f6000000000000000001aa", "question": ["Which SKUs are at risk of stockout this week?"]},
            {"id": "a1b2c3d4e5f6000000000000000002bb", "question": ["What is the forecast accuracy by product category?"]},
            {"id": "a1b2c3d4e5f6000000000000000003cc", "question": ["Show total shipment volume by warehouse for the last 3 months"]},
            {"id": "a1b2c3d4e5f6000000000000000004dd", "question": ["Which warehouses have fill rates below 95%?"]},
            {"id": "a1b2c3d4e5f6000000000000000005ee", "question": ["What is the average days-of-supply by product category?"]}
        ]
    },
    "data_sources": {
        "tables": [
            {
                "identifier": f"{FQN}.demand_forecasts",
                "description": ["ML-generated demand forecasts with confidence intervals and actuals for tracking prediction accuracy. Monthly granularity by product and region."],
                "column_configs": [
                    {"column_name": "forecast_date", "enable_format_assistance": True},
                    {"column_name": "model_version", "enable_format_assistance": True, "enable_entity_matching": True},
                    {"column_name": "product_category", "enable_format_assistance": True, "enable_entity_matching": True},
                    {"column_name": "region", "enable_format_assistance": True, "enable_entity_matching": True}
                ]
            },
            {
                "identifier": f"{FQN}.inventory_levels",
                "description": ["Daily inventory snapshots across 8 distribution centers. Tracks quantity on hand, reorder points, safety stock levels, and supplier lead times."],
                "column_configs": [
                    {"column_name": "product_category", "enable_format_assistance": True, "enable_entity_matching": True},
                    {"column_name": "product_sku", "enable_format_assistance": True, "enable_entity_matching": True},
                    {"column_name": "warehouse_id", "enable_format_assistance": True, "enable_entity_matching": True},
                    {"column_name": "warehouse_name", "enable_format_assistance": True, "enable_entity_matching": True}
                ]
            },
            {
                "identifier": f"{FQN}.shipment_orders",
                "description": ["Historical shipment order transactions from NorthStar Logistics. Contains order details including product, quantity, warehouse origin, destination region, and delivery status."],
                "column_configs": [
                    {"column_name": "destination_region", "enable_format_assistance": True, "enable_entity_matching": True},
                    {"column_name": "order_date", "enable_format_assistance": True},
                    {"column_name": "order_status", "enable_format_assistance": True, "enable_entity_matching": True},
                    {"column_name": "product_category", "enable_format_assistance": True, "enable_entity_matching": True},
                    {"column_name": "product_sku", "enable_format_assistance": True, "enable_entity_matching": True},
                    {"column_name": "warehouse_id", "enable_format_assistance": True, "enable_entity_matching": True}
                ]
            }
        ]
    },
    "instructions": {
        "text_instructions": [
            {
                "id": "01f0b37c378e1c9100000000000000a1",
                "content": [
                    "You are a supply chain analytics assistant for NorthStar Logistics, a nationwide 3PL provider with 8 distribution centers. ",
                    "When asked about stockout risk, check inventory_levels where quantity_on_hand <= reorder_point. ",
                    "When asked about critical stock, check where quantity_on_hand < safety_stock_qty. ",
                    "Days of Supply = quantity_on_hand / NULLIF(avg_daily_demand, 0). ",
                    "Inventory Turnover = total_units_shipped / NULLIF(avg_inventory, 0). ",
                    "Fill Rate = orders with status 'Delivered' / total orders * 100. ",
                    "Forecast Accuracy = 1 - ABS(actual_demand - predicted_demand) / NULLIF(actual_demand, 0). ",
                    "When asked about 'last month' or 'this month', use calendar months relative to the max date in the data. ",
                    "Round percentages to 1 decimal place and monetary values to 2 decimal places. ",
                    "The 8 warehouses are: WH-EAST-01 (Newark DC), WH-EAST-02 (Atlanta DC), WH-CENT-01 (Chicago DC), WH-CENT-02 (Minneapolis DC), WH-SOUTH-01 (Dallas DC), WH-SOUTH-02 (Miami DC), WH-WEST-01 (Los Angeles DC), WH-WEST-02 (Denver DC). ",
                    "Product categories are: Electronics, Food & Beverage, Home & Garden, Health & Wellness, Clothing & Apparel."
                ]
            }
        ],
        "example_question_sqls": [
            {
                "id": "01f0821116d912db00000000000000b1",
                "question": ["Which SKUs are currently below their reorder point?"],
                "sql": [
                    f"SELECT product_sku, product_name, warehouse_id, warehouse_name,\n",
                    f"  quantity_on_hand, reorder_point, safety_stock_qty,\n",
                    f"  CASE WHEN quantity_on_hand < safety_stock_qty THEN 'CRITICAL'\n",
                    f"       WHEN quantity_on_hand <= reorder_point THEN 'LOW' END as stock_status\n",
                    f"FROM {FQN}.inventory_levels\n",
                    f"WHERE quantity_on_hand <= reorder_point\n",
                    f"ORDER BY quantity_on_hand / NULLIF(reorder_point, 0) ASC"
                ]
            },
            {
                "id": "01f099751a3a1df300000000000000b2",
                "question": ["What is the total quantity shipped by warehouse for the last 3 months?"],
                "sql": [
                    f"SELECT warehouse_id, SUM(quantity) as total_shipped, COUNT(DISTINCT order_id) as total_orders\n",
                    f"FROM {FQN}.shipment_orders\n",
                    f"WHERE order_date >= ADD_MONTHS((SELECT MAX(order_date) FROM {FQN}.shipment_orders), -3)\n",
                    f"  AND order_status = 'Delivered'\n",
                    f"GROUP BY warehouse_id\n",
                    f"ORDER BY total_shipped DESC"
                ]
            },
            {
                "id": "01f099751a3a1df300000000000000b3",
                "question": ["Show forecast accuracy by product category"],
                "sql": [
                    f"SELECT product_category,\n",
                    f"  ROUND(AVG(100 - forecast_error_pct), 1) as avg_accuracy_pct,\n",
                    f"  ROUND(AVG(forecast_error_pct), 1) as avg_error_pct,\n",
                    f"  COUNT(*) as num_forecasts\n",
                    f"FROM {FQN}.demand_forecasts\n",
                    f"GROUP BY product_category\n",
                    f"ORDER BY avg_accuracy_pct DESC"
                ]
            },
            {
                "id": "01f099751a3a1df300000000000000b4",
                "question": ["What is the average days of supply by product category?"],
                "sql": [
                    f"WITH daily_demand AS (\n",
                    f"  SELECT product_sku, product_category,\n",
                    f"    SUM(quantity) / NULLIF(DATEDIFF((SELECT MAX(order_date) FROM {FQN}.shipment_orders),\n",
                    f"      (SELECT MIN(order_date) FROM {FQN}.shipment_orders)), 0) as avg_daily_demand\n",
                    f"  FROM {FQN}.shipment_orders\n",
                    f"  WHERE order_status = 'Delivered'\n",
                    f"  GROUP BY product_sku, product_category\n",
                    f")\n",
                    f"SELECT i.product_category,\n",
                    f"  ROUND(AVG(i.quantity_on_hand / NULLIF(d.avg_daily_demand, 0)), 1) as avg_days_of_supply\n",
                    f"FROM {FQN}.inventory_levels i\n",
                    f"JOIN daily_demand d ON i.product_sku = d.product_sku\n",
                    f"GROUP BY i.product_category\n",
                    f"ORDER BY avg_days_of_supply ASC"
                ]
            },
            {
                "id": "01f099751a3a1df300000000000000b5",
                "question": ["Compare actual vs forecasted demand for Electronics in Q4"],
                "sql": [
                    f"SELECT product_name, forecast_date,\n",
                    f"  predicted_demand, actual_demand,\n",
                    f"  actual_demand - predicted_demand as variance,\n",
                    f"  ROUND(forecast_error_pct, 1) as error_pct,\n",
                    f"  model_version\n",
                    f"FROM {FQN}.demand_forecasts\n",
                    f"WHERE product_category = 'Electronics'\n",
                    f"  AND forecast_date >= '2025-10-01'\n",
                    f"ORDER BY forecast_date, product_name"
                ]
            }
        ],
        "join_specs": [],
        "sql_snippets": {
            "filters": [
                {
                    "id": "01f09972e66d100000000000000000d1",
                    "sql": ["inventory.quantity_on_hand <= inventory.reorder_point"],
                    "display_name": "below reorder point",
                    "synonyms": ["low stock", "needs reorder", "stockout risk"],
                    "instruction": ["Use when the user asks about products that need reordering or are at stockout risk"]
                },
                {
                    "id": "01f09972e66d100000000000000000d2",
                    "sql": ["inventory.quantity_on_hand < inventory.safety_stock_qty"],
                    "display_name": "below safety stock",
                    "synonyms": ["critical stock", "emergency", "urgent reorder"],
                    "instruction": ["Use when the user asks about critically low inventory"]
                },
                {
                    "id": "01f09972e66d100000000000000000d3",
                    "sql": ["orders.order_status = 'Delivered'"],
                    "display_name": "delivered orders",
                    "synonyms": ["completed orders", "fulfilled orders"],
                    "instruction": ["Use when calculating fill rates, shipment volumes, or completed deliveries"]
                }
            ],
            "expressions": [
                {
                    "id": "01f09974563a100000000000000000e1",
                    "alias": "order_month",
                    "sql": ["DATE_TRUNC('month', orders.order_date)"],
                    "display_name": "month",
                    "synonyms": ["order month", "monthly"]
                },
                {
                    "id": "01f09974563a100000000000000000e2",
                    "alias": "order_quarter",
                    "sql": ["DATE_TRUNC('quarter', orders.order_date)"],
                    "display_name": "quarter",
                    "synonyms": ["order quarter", "quarterly"]
                },
                {
                    "id": "01f09974563a100000000000000000e3",
                    "alias": "stock_status",
                    "sql": ["CASE WHEN inventory.quantity_on_hand < inventory.safety_stock_qty THEN 'Critical' WHEN inventory.quantity_on_hand <= inventory.reorder_point THEN 'Low' WHEN inventory.quantity_on_hand <= inventory.reorder_point * 2 THEN 'Healthy' ELSE 'Overstocked' END"],
                    "display_name": "stock status",
                    "synonyms": ["inventory status", "stock level classification"]
                }
            ],
            "measures": [
                {
                    "id": "01f09972611f100000000000000000f1",
                    "alias": "total_shipped",
                    "sql": ["SUM(orders.quantity)"],
                    "display_name": "total units shipped",
                    "synonyms": ["total quantity", "shipment volume", "units shipped"]
                },
                {
                    "id": "01f09972611f100000000000000000f2",
                    "alias": "total_revenue",
                    "sql": ["SUM(orders.quantity * orders.unit_price)"],
                    "display_name": "total revenue",
                    "synonyms": ["revenue", "total sales", "order value"]
                },
                {
                    "id": "01f09972611f100000000000000000f3",
                    "alias": "avg_forecast_accuracy",
                    "sql": ["ROUND(AVG(100 - forecasts.forecast_error_pct), 1)"],
                    "display_name": "average forecast accuracy",
                    "synonyms": ["forecast accuracy", "prediction accuracy", "model accuracy"]
                },
                {
                    "id": "01f09972611f100000000000000000f4",
                    "alias": "total_inventory_value",
                    "sql": ["SUM(inventory.quantity_on_hand * inventory.unit_cost)"],
                    "display_name": "total inventory value",
                    "synonyms": ["inventory value", "stock value", "warehouse value"]
                }
            ]
        }
    },
    "benchmarks": {
        "questions": [
            {
                "id": "01f0d0b4e81510000000000000000a01",
                "question": ["Which SKUs are currently below their reorder point?"],
                "answer": [{"format": "SQL", "content": [
                    f"SELECT product_sku, product_name, warehouse_id, warehouse_name, quantity_on_hand, reorder_point\n",
                    f"FROM {FQN}.inventory_levels\n",
                    f"WHERE quantity_on_hand <= reorder_point ORDER BY quantity_on_hand ASC"
                ]}]
            },
            {
                "id": "01f0d0b4e81510000000000000000a02",
                "question": ["What is the forecast accuracy by product category?"],
                "answer": [{"format": "SQL", "content": [
                    f"SELECT product_category, ROUND(AVG(100 - forecast_error_pct), 1) as accuracy_pct\n",
                    f"FROM {FQN}.demand_forecasts\n",
                    f"GROUP BY product_category ORDER BY accuracy_pct DESC"
                ]}]
            },
            {
                "id": "01f0d0b4e81510000000000000000a03",
                "question": ["Show total shipment volume by warehouse"],
                "answer": [{"format": "SQL", "content": [
                    f"SELECT warehouse_id, SUM(quantity) as total_shipped\n",
                    f"FROM {FQN}.shipment_orders\n",
                    f"WHERE order_status = 'Delivered'\n",
                    f"GROUP BY warehouse_id ORDER BY total_shipped DESC"
                ]}]
            },
            {
                "id": "01f0d0b4e81510000000000000000a04",
                "question": ["What is the total inventory value by warehouse?"],
                "answer": [{"format": "SQL", "content": [
                    f"SELECT warehouse_id, warehouse_name,\n",
                    f"  ROUND(SUM(quantity_on_hand * unit_cost), 2) as total_value\n",
                    f"FROM {FQN}.inventory_levels\n",
                    f"GROUP BY warehouse_id, warehouse_name ORDER BY total_value DESC"
                ]}]
            },
            {
                "id": "01f0d0b4e81510000000000000000a05",
                "question": ["Which products need reordering today based on lead time?"],
                "answer": [{"format": "SQL", "content": [
                    f"SELECT product_sku, product_name, warehouse_name,\n",
                    f"  quantity_on_hand, reorder_point, lead_time_days\n",
                    f"FROM {FQN}.inventory_levels\n",
                    f"WHERE quantity_on_hand <= reorder_point\n",
                    f"ORDER BY lead_time_days DESC, quantity_on_hand ASC"
                ]}]
            }
        ]
    }
}

payload = {
    "title": "NorthStar Logistics - Demand Forecasting & Inventory",
    "description": (
        "Supply chain analytics for NorthStar Logistics. "
        "Ask questions about demand forecasts, inventory levels, stockout risks, "
        "fill rates, and shipment trends across 8 distribution centers and 50 SKUs."
    ),
    "parent_path": f"/Workspace/Users/{username}",
    "warehouse_id": WH_ID,
    "serialized_space": json.dumps(serialized_space)
}

resp = requests.post(
    f"{api_base}/api/2.0/genie/spaces",
    headers=headers,
    json=payload
)

if resp.status_code == 200:
    space = resp.json()
    space_id = space["space_id"]
    print(f"Genie space created: {space['title']}")
    print(f"Space ID: {space_id}")
else:
    print(f"Error creating Genie space ({resp.status_code}): {resp.text}")
    space_id = None

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9 · Summary

# COMMAND ----------

print("=" * 70)
print("  SETUP COMPLETE")
print("=" * 70)
print()
print(f"  Catalog : {CATALOG}")
print(f"  Schema  : {FQN}")
print()

for t in ["shipment_orders", "inventory_levels", "demand_forecasts"]:
    cnt = spark.table(f"{FQN}.{t}").count()
    print(f"  {t:30s}  {cnt:>6,} rows")

print()
if WH_ID and space_id:
    genie_url = f"https://{workspace_url}/genie/rooms/{space_id}"
    print(f"  Genie Space URL:")
    print(f"  {genie_url}")
    displayHTML(f'<h3><a href="{genie_url}" target="_blank">Open Genie Space</a></h3>')
else:
    print("  Genie space was not created (no warehouse_id provided).")
