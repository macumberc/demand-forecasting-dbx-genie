# Genie Data Room — AI Prompt Template

A reusable template for vibe-coding a complete Databricks Genie data room with AI agent assistance. Fill in the blanks, paste the result into your AI coding assistant, and let it build the whole thing end-to-end.

---

## How to Use

1. Copy the **Prompt** section below
2. Replace everything in `[brackets]` with your specifics
3. Paste into Cursor (Agent mode), Claude Code, or any AI coding assistant with tool access
4. Follow up with the **Phase prompts** in order — or paste the whole thing at once

---

## Step 0 — Fill in Your Scenario

Answer these questions first, then plug the answers into the prompt below.

| # | Question | Example (from this repo) | Your Answer |
|---|----------|--------------------------|-------------|
| 1 | **What industry/domain?** | Logistics / Supply Chain | |
| 2 | **What business problem?** | Demand forecasting & inventory management | |
| 3 | **Who is the persona using the Genie room?** | Supply chain analyst at a 3PL provider | |
| 4 | **What fictional company name?** | NorthStar Logistics | |
| 5 | **How many tables? (2-5 recommended)** | 3 | |
| 6 | **What are the tables?** | shipment_orders, inventory_levels, demand_forecasts | |
| 7 | **What dimensions should the data span?** | 20 products, 8 warehouses, 5 categories, 12 months, 6 regions | |
| 8 | **What time range?** | Jan 2025 — Dec 2025 | |
| 9 | **What key metrics should Genie understand?** | Days of Supply, Fill Rate, Forecast Accuracy, Inventory Turnover | |
| 10 | **What are 5-8 sample questions a user would ask?** | "Which SKUs are at risk of stockout?" / "What is forecast accuracy by category?" | |
| 11 | **Target catalog name?** | (user's choice or default to username) | |
| 12 | **Should it be distributable?** | Yes — Git repo + pip install | |

---

## The Prompt

Copy everything below the line and paste it into your AI assistant.

---

### Phase 1 — Plan the Genie Data Room (Google Sheets)

```
Based on the following template for creating a Genie data room:
https://docs.google.com/spreadsheets/d/1w4FIx3IqhJjfsN4-mfVNEJR49_LE30bzY0XdXgh21So/edit?usp=sharing

Create a copy and populate it with a real-world scenario:

- Industry: [your industry, e.g., "healthcare", "retail", "financial services"]
- Business problem: [your problem, e.g., "patient readmission prediction", "churn analysis"]
- Persona: [who uses this, e.g., "revenue operations analyst", "clinical data scientist"]
- Company name: [fictional company, e.g., "MedVista Health Systems"]
- Tables (aim for 2-5):
  1. [table_name] — [one-line description]
  2. [table_name] — [one-line description]
  3. [table_name] — [one-line description]
- Dimensions: [e.g., "50 providers, 12 facilities, 24 months of data, 6 departments"]
- Time range: [e.g., "Jan 2024 — Dec 2025"]
- Key metrics Genie should know:
  - [metric 1 with formula, e.g., "Readmission Rate = readmissions within 30 days / total discharges"]
  - [metric 2]
  - [metric 3]
- Sample questions users would ask:
  1. [question 1]
  2. [question 2]
  3. [question 3]
  4. [question 4]
  5. [question 5]

Populate every tab in the template: Instructions, Scenario, Dataset 1-3, and Document URLs.
```

### Phase 2 — Deploy to Databricks

```
Deploy the datasets and a Genie room to my Databricks workspace.

1. Create a Unity Catalog catalog and schema for the data
2. Generate all tables with realistic synthetic data using SQL (CTAS statements).
   The data should have:
   - Seasonal patterns and realistic variance (not uniform random)
   - Proper relationships between tables (shared keys like SKUs, IDs, regions)
   - Realistic distributions (e.g., 80/20 Pareto for order volumes, seasonal spikes)
   - At least [target row count, e.g., "5,000+ total rows"] across all tables
3. Add column comments to every column describing what it contains and its valid values
4. Create a fully-configured Genie space on top of the tables with:
   - General text instructions explaining the business context and metric formulas
   - 5 sample questions displayed on the landing page
   - 5 example SQL queries covering the most common analysis patterns
   - SQL snippets: filters, expressions, and measures that map business terms to SQL
   - 5 benchmark questions with expected SQL answers
   - Column configs with entity matching and format assistance enabled on key dimensions
```

### Phase 3 — Scale the Data

```
Enhance the datasets so they have larger scale and more realistic patterns.
Increase the row counts significantly while maintaining data quality —
seasonal trends, proper distributions, and realistic variance.
Don't just multiply rows; make the data generation logic richer.
```

### Phase 4 — Package as a Distributable Repo

```
Wrap everything into a GitHub repo so it can be distributed.
DO NOT do any more work inside the Databricks workspace.

I want to give someone the repo link, have them clone it into a Databricks
Git folder, then run a single notebook that creates all the datasets and
the Genie room exactly as deployed.

The notebook should:
- Use dbutils widgets for catalog_name, schema_name, and warehouse_id
- Default catalog_name to the current user's name
- Skip Genie space creation if no warehouse_id is provided
- Print a clickable link to the Genie space at the end
- Have zero external dependencies beyond what's in a Databricks runtime
```

### Phase 5 — Package as pip-installable Library (optional)

```
Package this as a pip-installable Python library so someone can do:

  %pip install [your-package-name]

  from [your_package] import deploy
  result = deploy(spark, catalog="my_catalog", warehouse_id="abc123")

Requirements:
- Rename the notebook to avoid conflicts with setup.py
- Create a pyproject.toml with setuptools backend
- Extract all logic into a clean Python module with a single deploy() function
- The deploy() function should accept spark, catalog, schema, and warehouse_id
- Handle permission errors gracefully (not all users can CREATE CATALOG)
- Build the wheel and sdist, validate with twine check
- Update README with both pip install and Git folder instructions
```

---

## Tips from Experience

These are lessons learned from building this repo that will save you debugging time:

### Genie API Gotchas
- The `serialized_space` field is required and must be a JSON string (not an object)
- Tables in `data_sources.tables` **must be sorted alphabetically** by their `identifier`
- All IDs in the payload must be **32-character lowercase hex strings** (no letters past 'f')
- Join specs can be tricky — if the API rejects them, remove them and add joins via the UI later
- Use `requests.post` with the notebook's API token rather than the Databricks CLI for the Genie API

### Data Generation
- Use `EXPLODE(SEQUENCE(...))` to generate date ranges
- Use `CROSS JOIN` to create cartesian products of dimensions, then filter with `WHERE RAND() < probability` to get realistic sparsity
- Use `CASE WHEN` on `MONTH()` to inject seasonal patterns
- Use `SIN()` with `DAYOFYEAR()` for smooth cyclical patterns in inventory
- Use `HASH(CONCAT(key1, key2)) % 100` for deterministic-looking pseudo-random assignment

### Packaging
- `setup.py` conflicts with Python packaging — rename your notebook to `setup_notebook.py`
- Wrap `CREATE CATALOG` in try/except for permission errors — most shared workspaces restrict this
- The only non-stdlib dependency needed is `requests` (pyspark is already in Databricks runtimes)
- Use `spark.conf.get("spark.databricks.workspaceUrl")` for the API base URL
- Use `dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()` for auth

### Distribution
- Always provide two paths: Git folder (notebook) and pip install (library)
- Default the catalog name to the current user's username for zero-config deploys
- Make the Genie space optional (skip if no warehouse_id) so users can test just the tables first
