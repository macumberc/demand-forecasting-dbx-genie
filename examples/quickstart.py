# Databricks notebook source
# MAGIC %md
# MAGIC # Genie Space Generator - Quick Start
# MAGIC
# MAGIC Generate a fully-configured Genie space for any industry in under 5 minutes.
# MAGIC Includes synthetic data, metric views with `MEASURE()` syntax, sample questions,
# MAGIC example SQL, and benchmarks.

# COMMAND ----------

# MAGIC %pip install git+https://github.com/macumberc/genie-space-generator.git -q
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate a Healthcare Genie Space
# MAGIC
# MAGIC MedFlow Analytics is a regional hospital network with 12 facilities tracking
# MAGIC patient outcomes, bed utilization, and readmission rates. They need to reduce
# MAGIC 30-day readmissions (currently 18%) and optimize staffing across facilities.

# COMMAND ----------

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Open Your Genie Space
# MAGIC
# MAGIC Click the **Open Genie Space** button in the output above to start exploring.
# MAGIC Try asking questions like:
# MAGIC - "Which facilities have the highest readmission rates?"
# MAGIC - "Show me bed utilization trends by month"
# MAGIC - "What is the average length of stay by department?"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Try a Different Industry
# MAGIC
# MAGIC Uncomment and run the cell below to generate a Retail analytics space instead.

# COMMAND ----------

# from genie_space_generator import deploy
#
# retail_result = deploy(
#     spark,
#     industry="Retail",
#     company_name="ShopWise Analytics",
#     use_case="Inventory optimization and demand forecasting",
#     business_context="National retail chain with 200 stores across 8 regions. Managing "
#                      "50,000 SKUs with $2.1B annual revenue. Stockout rate of 4.2% is "
#                      "costing an estimated $45M in lost sales annually.",
# )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC
# MAGIC Run the cell below to remove all created resources (schema, tables, metric views, Genie space).

# COMMAND ----------

from genie_space_generator import teardown
teardown(spark, **result)
