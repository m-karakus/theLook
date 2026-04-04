"""Distribution assets — one @asset per mkpipe table.

Each table becomes its own Dagster asset so that the multiprocess
executor can run them in **parallel** (up to MAX_CONCURRENT_MKPIPE_RUNS).

Each distribution asset declares an explicit upstream dependency on
its source dbt/stg model via ``deps=[AssetKey(["dwh", table.name])]``.

Asset key: ["distribution", <target_name>]
Group:     distribution_<primary_tag>
"""

import logging
from collections import defaultdict

from dagster import (
    AssetDep,
    AssetExecutionContext,
    AssetKey,
    AssetsDefinition,
    RetryPolicy,
    asset,
)

from dagster_project.config.mkpipe_parser import MkpipeTable, PipelineInfo
from dagster_project.config.settings import HEAVY_TAG
from dagster_project.resources.mkpipe_resource import MkpipeResource

logger = logging.getLogger(__name__)


def build_distribution_assets(pipelines: list[PipelineInfo]) -> list[AssetsDefinition]:
    """Build one @asset per distribution table.

    Each asset is an independent unit of execution.  When a job selects
    multiple distribution assets, the multiprocess executor launches them
    in separate processes, achieving true parallelism.
    """
    assets: list[AssetsDefinition] = []
    distribution_pipelines = [p for p in pipelines if p.direction == 'distribution']

    for pipeline in distribution_pipelines:
        for table in pipeline.tables:
            asset_def = _create_distribution_asset(table)
            assets.append(asset_def)

    # Log summary grouped by tag
    tag_counts: dict[str, int] = defaultdict(int)
    for pipeline in distribution_pipelines:
        for table in pipeline.tables:
            primary_tag = table.tags[0] if table.tags else 'default'
            tag_counts[primary_tag] += 1

    logger.info(
        'Built %d distribution assets for tags: %s',
        len(assets),
        dict(tag_counts),
    )
    return assets


def _upstream_key(table: MkpipeTable) -> AssetKey:
    """Determine upstream asset key for a distribution table.

    Distribution tables read from Snowflake (DWH or DWH_STG schema).
    The data is produced by dbt models, so the upstream key is
    always AssetKey(["dwh", name]).
    """
    return AssetKey(['dwh', table.name])


def _create_distribution_asset(table: MkpipeTable) -> AssetsDefinition:
    """Create a single @asset for one distribution table."""
    primary_tag = table.tags[0] if table.tags else 'default'
    target_name = table.target_name
    upstream = _upstream_key(table)
    is_heavy = HEAVY_TAG in table.tags if table.tags else False
    tags = {HEAVY_TAG: 'true'} if is_heavy else {}

    @asset(
        name=target_name,
        key_prefix=['distribution'],
        group_name=f'distribution_{primary_tag}',
        deps=[AssetDep(upstream)],
        compute_kind='mkpipe',
        op_tags=tags,
        retry_policy=RetryPolicy(max_retries=2, delay=10),
        description=(
            f'Distribution: {table.source_connection}.{table.name} -> '
            f'{table.destination_connection}.{target_name} ({table.replication_method})'
        ),
    )
    def _asset_fn(context: AssetExecutionContext, mkpipe: MkpipeResource) -> None:
        context.log.info("Distributing table='%s' (heavy=%s)", target_name, is_heavy)
        mkpipe.run_table(target_name)

    return _asset_fn
