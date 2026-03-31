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

    Exposure tags are defined in exposures.yml via config.tags.
    """

    dbt_select: str
    cron: str
    description: str


# Exposure groups drive Dagster job selection.
# Each group uses dbt tag/exposure-based selection:
#   +tag:<name> selects all nodes tagged with <name> plus upstream chain.
#   +exposure:<name> selects all upstream models of a specific exposure.
EXPOSURE_GROUPS: dict[str, ExposureGroupConfig] = {
    'reporting': ExposureGroupConfig(
        dbt_select='+tag:reporting',
        cron='0 6 * * *',  # Her gun saat 06:00 UTC
        description='theLook e-commerce reporting pipeline - daily',
    ),
}

# --- Dagster concurrency ---
MAX_CONCURRENT_MKPIPE_RUNS = int(os.environ.get('MKPIPE_MAX_CONCURRENT', '4'))
MAX_CONCURRENT_HEAVY_RUNS = int(os.environ.get('MKPIPE_MAX_CONCURRENT_HEAVY', '1'))
HEAVY_TAG = 'heavy'
