"""Ingestion assets — one @asset per mkpipe table.

Each table becomes its own Dagster asset so that the multiprocess
executor can run them in **parallel** (up to MAX_CONCURRENT_MKPIPE_RUNS).

Asset key: ["ingestion", <target_name>]
Group:     ingestion_<primary_tag>
"""

import logging
from collections import defaultdict

from dagster import AssetExecutionContext, AssetsDefinition, RetryPolicy, asset

from dagster_project.config.mkpipe_parser import MkpipeTable, PipelineInfo
from dagster_project.config.settings import HEAVY_TAG
from dagster_project.resources.mkpipe_resource import MkpipeResource

logger = logging.getLogger(__name__)


def build_ingestion_assets(pipelines: list[PipelineInfo]) -> list[AssetsDefinition]:
    """Build one @asset per ingestion table.

    Each asset is an independent unit of execution.  When a job selects
    multiple ingestion assets, the multiprocess executor launches them
    in separate processes, achieving true parallelism.
    """
    assets: list[AssetsDefinition] = []
    ingestion_pipelines = [p for p in pipelines if p.direction == 'ingestion']

    for pipeline in ingestion_pipelines:
        for table in pipeline.tables:
            asset_def = _create_ingestion_asset(table)
            assets.append(asset_def)

    # Log summary grouped by tag
    tag_counts: dict[str, int] = defaultdict(int)
    for pipeline in ingestion_pipelines:
        for table in pipeline.tables:
            primary_tag = table.tags[0] if table.tags else 'default'
            tag_counts[primary_tag] += 1

    logger.info(
        'Built %d ingestion assets for tags: %s',
        len(assets),
        dict(tag_counts),
    )
    return assets


def _create_ingestion_asset(table: MkpipeTable) -> AssetsDefinition:
    """Create a single @asset for one ingestion table."""
    primary_tag = table.tags[0] if table.tags else 'default'
    target_name = table.target_name
    is_heavy = HEAVY_TAG in table.tags if table.tags else False
    tags = {HEAVY_TAG: 'true'} if is_heavy else {}

    @asset(
        name=target_name,
        key_prefix=['ingestion'],
        group_name=f'ingestion_{primary_tag}',
        compute_kind='mkpipe',
        op_tags=tags,
        retry_policy=RetryPolicy(max_retries=2, delay=10),
        description=(
            f'Ingestion: {table.source_connection}.{table.name} -> '
            f'STG.{target_name} ({table.replication_method})'
        ),
    )
    def _asset_fn(context: AssetExecutionContext, mkpipe: MkpipeResource) -> None:
        context.log.info("Ingesting table='%s' (heavy=%s)", target_name, is_heavy)
        mkpipe.run_table(target_name)

    return _asset_fn
