# Databricks notebook source
# MAGIC %md
# MAGIC # E2E Test: Genie Space Generator
# MAGIC Deploys a Genie space, verifies outputs, then tears everything down.

# COMMAND ----------

# MAGIC %pip install git+https://github.com/macumberc/demand-forecasting-dbx-genie.git@automated-creation -q
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from genie_space_generator import deploy

result = deploy(
    spark,
    industry="Healthcare",
    company_name="E2E Test Hospital",
    use_case="Patient readmission analysis",
    business_context="Small test hospital network with 4 facilities tracking patient outcomes "
                     "and readmission rates for quality improvement.",
    num_tables=2,
    num_products=10,
    num_locations=4,
    scale=1,
)

print("=== DEPLOY RESULT ===")
for k, v in result.items():
    print(f"  {k}: {v}")

# COMMAND ----------

# Verify outputs
assert result.get("fqn"), "Missing fqn"
assert result.get("tables"), "No tables created"
assert len(result["tables"]) == 2, f"Expected 2 tables, got {len(result['tables'])}"
assert all(cnt > 0 for cnt in result["tables"].values()), "Some tables have 0 rows"
assert result.get("metric_view_fqdns"), "No metric views created"
assert len(result["metric_view_fqdns"]) == 2, f"Expected 2 metric views, got {len(result['metric_view_fqdns'])}"

genie = result.get("genie", {})
print(f"Genie status: {genie.get('status')}")
print(f"Genie URL: {result.get('genie_url', 'N/A')}")

if genie.get("status") in ("created", "replaced"):
    print("GENIE SPACE CREATED SUCCESSFULLY")
else:
    print(f"WARNING: Genie space status = {genie.get('status')}")

print("\nALL DEPLOY ASSERTIONS PASSED")

# COMMAND ----------

# Teardown
from genie_space_generator import teardown

cleanup = teardown(spark, **result)
print("=== CLEANUP RESULT ===")
print(cleanup)
print("\nTEARDOWN COMPLETE")
