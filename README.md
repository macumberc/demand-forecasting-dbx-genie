# Genie Space Generator

[![CI](https://github.com/macumberc/genie-space-generator/actions/workflows/ci.yml/badge.svg)](https://github.com/macumberc/genie-space-generator/actions/workflows/ci.yml)

Generate a fully-configured Databricks Genie space for **any industry and use case**. Enter an industry, company name, and business context — get synthetic data, metric views, and a curated Genie space in under 5 minutes.

## Quick Start

```python
# Cell 1 — Install
%pip install git+https://github.com/macumberc/genie-space-generator.git -q
dbutils.library.restartPython()
```

```python
# Cell 2 — Deploy
from genie_space_generator import deploy

result = deploy(
    spark,
    industry="Healthcare",
    company_name="MedFlow Analytics",
    use_case="Patient readmission prediction and hospital capacity planning",
    business_context="Regional hospital network with 12 facilities tracking patient outcomes, "
                     "bed utilization, and readmission rates to reduce 30-day readmissions "
                     "(currently 18%) and optimize staffing.",
)
```

Click **Open Genie Space** in the output to start exploring.

### Cleanup

```python
from genie_space_generator import teardown
teardown(spark, **result)
```

## What It Creates

For each deployment, the generator produces:

- **N tables** (default 3) — transaction, snapshot, and forecast archetypes adapted to the domain
- **N metric views** — one per table with 4-6 governed measures using `MEASURE()` syntax
- **Genie space** with:
  - All tables + metric views as data sources
  - Domain-specific instructions
  - 7 sample questions on the landing page
  - 7 example SQL queries (including `MEASURE()` queries)
  - SQL snippets (3 filters, 3 expressions, 4 measures)
  - 7 benchmarks with expected SQL

## Databricks App (optional UI)

For teams that want a persistent web UI, deploy as a Databricks App with Gradio:

```bash
databricks apps create genie-space-generator --source-code-path . --config-file app.yaml
databricks apps deploy genie-space-generator
```

Users open the app URL, fill in four fields (Industry, Company Name, Use Case, Business Context), and click **Generate Genie Space**.

## How It Works

1. **LLM generates the domain model** — calls the Databricks Foundation Model API to produce a complete data specification (tables, columns, metric views, Genie instructions, sample questions, benchmarks) tailored to the user's industry and use case
2. **Deterministic SQL creates the data** — hash-based synthetic data generation (same seed = same data) produces realistic tables with seasonal patterns
3. **Metric views provide governed KPIs** — YAML-based metric views with `MEASURE()` syntax for each table
4. **Genie space is fully configured** — data sources, text instructions, example SQL queries, SQL snippets, and benchmarks

## API Reference

### `deploy(spark, industry, company_name, use_case, business_context, ...)`

| Parameter | Type | Default | Description |
|---|---|---|---|
| `spark` | SparkSession | *required* | Active Spark session |
| `industry` | str | *required* | Industry vertical |
| `company_name` | str | *required* | Company name for the demo |
| `use_case` | str | *required* | Analytics use case description |
| `business_context` | str | *required* | 2-3 sentence business scenario |
| `num_tables` | int | `3` | Number of tables to generate |
| `num_products` | int | `20` | Number of entity items per dimension |
| `num_locations` | int | `8` | Number of locations/facilities |
| `catalog` | str | current catalog | Target catalog name |
| `schema` | str | auto-generated | Target schema name |
| `warehouse_id` | str | `"auto"` | SQL warehouse ID; `"auto"` selects best available, `None` skips Genie |
| `seed` | int | `20250306` | Deterministic seed |
| `scale` | int | `1` | Data size multiplier (years of data) |

**Returns** a dict with keys: `catalog`, `schema`, `fqn`, `seed`, `tables`, `table_fqdns`, `metric_view_fqdns`, `warehouse_id`, `genie`, `genie_url`.

### `teardown(spark, **result)`

Accepts the dict returned by `deploy()`. Drops the schema (CASCADE) and deletes the Genie space.

## Prerequisites

- Databricks workspace with Unity Catalog enabled
- Permission to create schemas in a catalog
- A SQL Pro or Serverless SQL warehouse
- DBR 17.2+ or serverless notebook (metric views require DBR 17.2+)
- Access to Foundation Model API endpoint (`databricks-claude-sonnet-4-6`)
