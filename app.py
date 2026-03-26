"""Gradio frontend for the Genie Space Generator — deployed as a Databricks App."""

import logging

import gradio as gr

from genie_space_generator import deploy, teardown

# Configure logging so deploy() output appears in the app console
logging.basicConfig(level=logging.INFO, format="[genie-space-generator] %(message)s")


def _get_spark():
    """Get a Spark session via Databricks Connect."""
    from databricks.connect import DatabricksSession

    return DatabricksSession.builder.getOrCreate()


def generate(
    industry,
    company_name,
    use_case,
    business_context,
    num_tables,
    num_products,
    num_locations,
    scale,
    catalog,
):
    """Called when the user clicks Generate."""

    if not industry or not company_name or not use_case or not business_context:
        return "Please fill in all required fields.", None

    spark = _get_spark()

    result = deploy(
        spark,
        industry=industry,
        company_name=company_name,
        use_case=use_case,
        business_context=business_context,
        num_tables=int(num_tables),
        num_products=int(num_products),
        num_locations=int(num_locations),
        catalog=catalog or None,
        scale=int(scale),
    )

    genie_url = result.get("genie_url", "")
    tables = result.get("tables", {})
    table_summary = "\n".join(
        f"  - **{name}**: {count:,} rows" for name, count in tables.items()
    )
    total = sum(tables.values())
    mv_count = len(result.get("metric_view_fqdns", {}))

    summary = f"""## Setup Complete

**Schema:** `{result['fqn']}`

**Tables:**
{table_summary}

**Total:** {total:,} rows | **Metric Views:** {mv_count}
"""
    if genie_url:
        summary += f"\n### [Open Genie Space]({genie_url})"

    return summary, result


def cleanup(result_state):
    """Called when the user clicks Teardown."""

    if not result_state:
        return "No deployment to clean up."

    spark = _get_spark()
    teardown(spark, **result_state)
    return f"Cleaned up schema `{result_state.get('fqn')}` and Genie space."


with gr.Blocks(title="Genie Space Generator", theme=gr.themes.Soft()) as app:
    gr.Markdown(
        "# Genie Space Generator\n"
        "Generate a fully-configured Databricks Genie space for any industry and use case."
    )

    result_state = gr.State(None)

    with gr.Row():
        with gr.Column(scale=1):
            industry = gr.Textbox(
                label="Industry",
                placeholder="e.g., Healthcare, Retail, Manufacturing",
            )
            company_name = gr.Textbox(
                label="Company Name",
                placeholder="e.g., MedFlow Analytics",
            )
            use_case = gr.Textbox(
                label="Use Case",
                placeholder="e.g., Patient readmission prediction and hospital capacity planning",
            )
            business_context = gr.Textbox(
                label="Business Context",
                placeholder="Describe the business scenario in 2-3 sentences...",
                lines=4,
            )

        with gr.Column(scale=1):
            with gr.Accordion("Advanced Settings", open=False):
                num_tables = gr.Slider(
                    minimum=2, maximum=5, value=3, step=1, label="Number of Tables"
                )
                num_products = gr.Slider(
                    minimum=10, maximum=50, value=20, step=5,
                    label="Number of Entities/Products",
                )
                num_locations = gr.Slider(
                    minimum=4, maximum=16, value=8, step=2,
                    label="Number of Locations/Facilities",
                )
                scale = gr.Slider(
                    minimum=1, maximum=10, value=1, step=1, label="Data Scale (years)"
                )
                catalog = gr.Textbox(
                    label="Catalog (optional)",
                    placeholder="Leave blank for workspace default",
                )

            generate_btn = gr.Button(
                "Generate Genie Space", variant="primary", size="lg"
            )
            teardown_btn = gr.Button("Teardown", variant="stop", size="sm")

    output = gr.Markdown(label="Result")

    generate_btn.click(
        fn=generate,
        inputs=[
            industry, company_name, use_case, business_context,
            num_tables, num_products, num_locations, scale, catalog,
        ],
        outputs=[output, result_state],
    )
    teardown_btn.click(
        fn=cleanup,
        inputs=[result_state],
        outputs=[output],
    )

app.launch(server_name="0.0.0.0", server_port=8080)
