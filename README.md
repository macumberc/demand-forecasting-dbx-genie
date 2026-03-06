# Demand Forecasting & Inventory Management — Genie Data Room

A ready-to-deploy Databricks Genie space for logistics demand forecasting and inventory management. Two lines in a serverless notebook gives you a fully configured Genie room backed by three synthetic Unity Catalog tables.

## Quick Start

```python
# Cell 1
%pip install git+https://github.com/macumberc/demand-forecasting.git

# Cell 2
from demand_forecasting_genie import deploy
result = deploy(spark)
```

That's it. The `deploy()` function creates the catalog, schema, three tables (~6,700 rows), auto-detects the best SQL warehouse, creates a Genie space, and renders clickable buttons for the Genie room and cleanup.

To use a specific warehouse instead of auto-detection:

```python
result = deploy(spark, warehouse_id="your_warehouse_id")
```

## Cleanup

```python
from demand_forecasting_genie import teardown
teardown(spark, **result)
```

This drops the schema (CASCADE) and deletes the Genie space.

## Scenario

**NorthStar Logistics** is a nationwide third-party logistics (3PL) provider operating 8 regional distribution centers across the US. The supply chain team needs real-time visibility into demand patterns, inventory health, and forecast accuracy to reduce stockouts ($2.3M/year) and free up working capital tied in excess inventory ($5M+).

## API Reference

### `deploy(spark, catalog=None, schema="demand_forecasting", warehouse_id=None)`

| Parameter | Type | Default | Description |
|---|---|---|---|
| `spark` | SparkSession | *required* | Active Spark session (`spark` in notebooks) |
| `catalog` | str | current user | Target catalog name (e.g. `jane_doe`) |
| `schema` | str | `demand_forecasting` | Target schema name |
| `warehouse_id` | str | auto-detected | SQL warehouse ID for Genie space; auto-selects the best available if omitted |

**Returns** a dict with keys: `catalog`, `schema`, `fqn`, `tables`, `genie_url`.

### `teardown(spark, catalog=None, schema="demand_forecasting", genie_url=None, **kwargs)`

Accepts the same dict returned by `deploy()` via `**result` unpacking.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `spark` | SparkSession | *required* | Active Spark session |
| `catalog` | str | current user | Catalog containing the schema |
| `schema` | str | `demand_forecasting` | Schema to drop |
| `genie_url` | str | `None` | Genie URL to delete; skips if omitted |

**Returns** a dict with keys: `schema_dropped`, `genie_deleted`.

## What Gets Created

### Tables

| Table | Rows | Description |
|---|---|---|
| `shipment_orders` | ~1,400 | Order transactions across 20 SKUs, 8 warehouses, 12 months (Jan-Dec 2025) with seasonal demand patterns |
| `inventory_levels` | ~4,400 | Weekly inventory snapshots — quantity on hand, reorder points, safety stock, lead times |
| `demand_forecasts` | ~870 | Monthly ML forecasts with confidence intervals, actuals, and error tracking across 3 model versions |

### Products (20 SKUs across 5 categories)

| Category | SKUs | Example |
|---|---|---|
| Electronics | 5 | Wireless Bluetooth Speaker, USB-C Hub, Noise-Cancelling Earbuds |
| Food & Beverage | 4 | Organic Protein Bars, Cold Brew Coffee, Sparkling Water |
| Home & Garden | 4 | Smart LED Bulbs, Bamboo Cutting Board, Indoor Herb Garden Kit |
| Health & Wellness | 4 | Vitamin D3, Melatonin Gummies, Probiotic Capsules |
| Clothing & Apparel | 3 | Running Socks, Compression Tights, Hiking Shorts |

### Warehouses (8 distribution centers)

| ID | Name | Region |
|---|---|---|
| WH-EAST-01 | Newark DC | Northeast |
| WH-EAST-02 | Atlanta DC | Southeast |
| WH-CENT-01 | Chicago DC | Midwest |
| WH-CENT-02 | Minneapolis DC | Great Lakes |
| WH-SOUTH-01 | Dallas DC | South Central |
| WH-SOUTH-02 | Miami DC | Southeast |
| WH-WEST-01 | Los Angeles DC | Pacific |
| WH-WEST-02 | Denver DC | Mountain |

### Genie Space Configuration

The Genie space is deployed with:

- **General instructions** — business context, metric definitions (Days of Supply, Fill Rate, Forecast Accuracy, Inventory Turnover), warehouse mapping
- **5 sample questions** displayed to users on the landing page
- **5 example SQL queries** covering stockout risk, shipment volume, forecast accuracy, days-of-supply, and actual vs. predicted demand
- **SQL snippets** — 3 filters, 3 expressions, 4 measures
- **5 benchmarks** with expected SQL for accuracy testing

## Example Questions to Ask Genie

- Which SKUs are at risk of stockout this week?
- What is the forecast accuracy by product category?
- Show total shipment volume by warehouse for the last 3 months.
- Which warehouses have fill rates below 95%?
- What is the average days-of-supply by product category?
- Compare actual vs forecasted demand for Electronics in Q4.
- What is the total inventory value by warehouse?
- Which products need reordering today based on lead time?

## Prerequisites

- Databricks workspace with Unity Catalog enabled
- Permission to create catalogs (or an existing catalog you can write to)
- A SQL Pro or Serverless SQL warehouse (only needed for Genie space creation)
- DBR 13.3+ or serverless notebook

## Resources

- [Curate an effective Genie space](https://docs.databricks.com/aws/en/genie/best-practices)
- [How to Build Production-Ready Genie Spaces](https://www.databricks.com/blog/how-build-production-ready-genie-spaces-and-build-trust-along-way)
- [Unity Catalog metric views](https://docs.databricks.com/aws/en/metric-views/)
- [Genie Data Room Planning Template (Google Sheets)](https://docs.google.com/spreadsheets/d/1w4FIx3IqhJjfsN4-mfVNEJR49_LE30bzY0XdXgh21So/edit)
