from __future__ import annotations

import logging

from dagster import Definitions
from dagster_dbt import DbtCliResource

from dagster_project.assets.dbt_assets import dwh_dbt_assets
from dagster_project.assets.dbt_operations import (
    create_iceberg_tables,
    refresh_all_iceberg_tables,
)
from dagster_project.assets.distribution import build_distribution_assets
from dagster_project.assets.ingestion import build_ingestion_assets
from dagster_project.config.mkpipe_parser import parse_mkpipe_config
from dagster_project.config.settings import (
    DBT_PROJECT_DIR,
    MKPIPE_CONFIG_PATH,
    MKPIPE_ENVIRONMENT,
)
from dagster_project.jobs import build_exposure_jobs
from dagster_project.resources.mkpipe_resource import MkpipeResource
from dagster_project.schedules import build_schedules

logger = logging.getLogger(__name__)

# --- Parse mkpipe config at module load time ---
pipelines = parse_mkpipe_config(MKPIPE_CONFIG_PATH, environment=MKPIPE_ENVIRONMENT)

# --- Build dynamic assets from config ---
ingestion_assets = build_ingestion_assets(pipelines)
distribution_assets = build_distribution_assets(pipelines)

all_assets = [
    dwh_dbt_assets,  # dbt models (auto-discovered from manifest)
    *ingestion_assets,  # mkpipe ingestion (source -> STG)
    *distribution_assets,  # mkpipe distribution (DWH -> Mongo/PG)
    create_iceberg_tables,  # manual: create missing iceberg tables
    refresh_all_iceberg_tables,  # manual: bulk refresh all iceberg tables
]

logger.info(
    'Loaded %d total assets: %d ingestion, 1 dbt, %d distribution',
    len(all_assets),
    len(ingestion_assets),
    len(distribution_assets),
)

# --- Build jobs and schedules (exposure-driven) ---
jobs = build_exposure_jobs()
schedules = build_schedules(jobs)

# --- Dagster Definitions ---
defs = Definitions(
    assets=all_assets,
    jobs=jobs,
    schedules=schedules,
    resources={
        'mkpipe': MkpipeResource(config_path=str(MKPIPE_CONFIG_PATH)),
        'dbt': DbtCliResource(
            project_dir=str(DBT_PROJECT_DIR),
            profiles_dir=str(DBT_PROJECT_DIR / '.dbt'),
        ),
    },
)
