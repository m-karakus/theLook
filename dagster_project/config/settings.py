from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

# --- Project paths ---
PROJECT_ROOT = Path(__file__).resolve().parents[2]
MKPIPE_CONFIG_PATH = PROJECT_ROOT / 'extract_load_project' / 'mkpipe_project.yaml'
DBT_PROJECT_DIR = PROJECT_ROOT / 'dbt_project'

# --- mkpipe environment ---
MKPIPE_ENVIRONMENT = 'prod'


# --- Exposure-driven pipeline configuration ---
@dataclass(frozen=True)
class ExposureGroupConfig:
    """Configuration for a functional exposure group.

    Each group maps to one Dagster job and one schedule.
    The dbt_select field uses dbt selection syntax to resolve
    all upstream models from the exposure(s).
    The distribution_dbt_select field selects ONLY the exposure's
    direct models (mart layer) — used to find downstream distribution
    assets without traversing shared stg/int models.

    Exposure tags are defined in exposures.yml via config.tags.
    Use `+tag:<name>` to select all upstream models of exposures
    tagged with that name.
    """

    dbt_select: str
    distribution_dbt_select: str
    cron: str
    description: str


# Exposure groups drive Dagster job selection.
# Each group uses dbt tag-based exposure selection:
#   +tag:<name> selects all nodes tagged with <name> (including exposures)
#   plus their entire upstream dependency chain.
#
# Tags are defined on each exposure in exposures.yml:
#   config:
#     tags: ['tableau']
#
# To add a new exposure to a group, simply add the tag to the
# exposure's config in exposures.yml. No changes needed here.
EXPOSURE_GROUPS: dict[str, ExposureGroupConfig] = {
    'api': ExposureGroupConfig(
        dbt_select='+exposure:dwh_api',
        distribution_dbt_select='1+exposure:dwh_api',
        cron='0 6,10,14,18 * * *',
        description='API pipeline - MongoDB endpoints',
    ),
    'tableau': ExposureGroupConfig(
        dbt_select='+tag:tableau',
        distribution_dbt_select='tag:tableau',
        cron='0 5 * * *',
        description='Tableau BI dashboards',
    ),
    'marketing': ExposureGroupConfig(
        dbt_select='+exposure:marketing_analytics',
        distribution_dbt_select='1+exposure:marketing_analytics',
        cron='0 7,15 * * *',
        description='Marketing analytics pipeline',
    ),
}

# --- Dagster concurrency ---
# Maximum number of assets (tables) that can run in parallel within a job.
# Override with MKPIPE_MAX_CONCURRENT env var per environment.
# Memory budget: each process ≈ driver_memory + executor_memory + 10% overhead.
MAX_CONCURRENT_MKPIPE_RUNS = int(os.environ.get('MKPIPE_MAX_CONCURRENT', '4'))

# Maximum number of "heavy" tables that can run in parallel.
# Heavy tables (prod_char_val, rated_tx, prod) use Iceberg FanoutWriter
# which consumes significant memory per partition. Limiting concurrency
# prevents OOM. This is enforced via executor tag_concurrency_limits.
MAX_CONCURRENT_HEAVY_RUNS = int(os.environ.get('MKPIPE_MAX_CONCURRENT_HEAVY', '1'))

# mkpipe tag that marks memory-intensive tables.
HEAVY_TAG = 'heavy'
