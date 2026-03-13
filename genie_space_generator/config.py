"""Package-wide configuration for the Genie space generator."""

from __future__ import annotations

PACKAGE_NAME = "genie-space-generator"
PACKAGE_VERSION = "1.0.0"

DEFAULT_SEED = 20250306
AUTO_WAREHOUSE = "auto"
HTTP_TIMEOUT_SECONDS = 30

SPACE_DESCRIPTION_MARKER = "Managed by genie_space_generator"

# LLM endpoint for domain generation
DEFAULT_LLM_ENDPOINT = "databricks-claude-sonnet-4-6"
LLM_MAX_TOKENS = 8000
LLM_TEMPERATURE = 0.3
LLM_MAX_RETRIES = 2
