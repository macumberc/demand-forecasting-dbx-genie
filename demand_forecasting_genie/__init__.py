"""Demand Forecasting & Inventory Management — Genie Data Room for Databricks."""

from .deploy import deploy, teardown

try:
    from importlib.metadata import version as _version

    __version__ = _version("demand-forecasting-genie")
except Exception:
    __version__ = "0.0.0-dev"

__all__ = ["deploy", "teardown"]
