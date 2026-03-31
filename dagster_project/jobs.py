from __future__ import annotations

from dagster import AssetSelection, define_asset_job, multiprocess_executor
from dagster_dbt import build_dbt_asset_selection

from dagster_project.assets.dbt_assets import dwh_dbt_assets
from dagster_project.config.settings import (
    EXPOSURE_GROUPS,
    HEAVY_TAG,
    MAX_CONCURRENT_HEAVY_RUNS,
    MAX_CONCURRENT_MKPIPE_RUNS,
)


def build_exposure_jobs() -> list:
    """Build one asset job per exposure group.

    Each job selects:
    - All dbt models upstream of the group's exposure(s)
      (resolved via dbt's native graph traversal)
    - Plus upstream mkpipe ingestion assets
    - Plus downstream mkpipe distribution assets

    Jobs use multiprocess executor to run up to MAX_CONCURRENT_MKPIPE_RUNS
    assets in parallel (e.g., 8 ingestion tables at the same time).
    """
    jobs = []

    executor = multiprocess_executor.configured(
        {
            'max_concurrent': MAX_CONCURRENT_MKPIPE_RUNS,
            'tag_concurrency_limits': [
                {
                    'key': HEAVY_TAG,
                    'limit': MAX_CONCURRENT_HEAVY_RUNS,
                },
            ],
        }
    )

    for group_name, config in EXPOSURE_GROUPS.items():
        # Select all dbt models that feed the exposure(s).
        # dbt_select="+exposure:dwh_api" resolves mart + int + base + stg models.
        dbt_selection = build_dbt_asset_selection(
            [dwh_dbt_assets],
            dbt_select=config.dbt_select,
        )

        # Extend selection to include the full pipeline chain:
        # - .upstream() adds mkpipe ingestion assets (ingestion/* -> dbt sources)
        # - distribution assets that depend on selected dbt models
        #
        # NOTE: We must NOT use dbt_selection.downstream() here because stg/int
        # models are shared across exposures. Dagster's .downstream() traverses
        # the entire asset graph, so it would pull in dbt models from OTHER
        # exposures (e.g., stg_dce__cust -> tableau models), causing the job
        # to select nearly all dbt models instead of just the exposure's subset.
        #
        # Instead, we intersect .downstream() with only distribution/* assets
        # to get the specific distribution assets linked to this exposure.
        distribution_selection = (
            dbt_selection.downstream() & AssetSelection.key_prefixes('distribution')
        )

        full_selection = (
            dbt_selection | dbt_selection.upstream() | distribution_selection
        )

        job = define_asset_job(
            name=f'{group_name}_pipeline_job',
            selection=full_selection,
            executor_def=executor,
            description=config.description,
        )
        jobs.append(job)

    return jobs
