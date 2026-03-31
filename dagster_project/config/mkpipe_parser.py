from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

# Variants considered as DWH targets (ingestion destination)
DWH_TARGET_VARIANTS = frozenset({'snowflake', 'clickhouse'})
# Variants considered as DWH sources (distribution source)
DWH_SOURCE_VARIANTS = frozenset({'snowflake', 'clickhouse'})


@dataclass(frozen=True)
class MkpipeTable:
    """Represents a single table entry from a mkpipe pipeline."""

    name: str
    target_name: str
    replication_method: str
    pipeline_name: str
    source_connection: str
    destination_connection: str
    direction: str  # "ingestion" | "distribution"
    tags: list[str] = field(default_factory=list)
    fetchsize: int = 1000
    custom_sql: str | None = None


@dataclass(frozen=True)
class PipelineInfo:
    """Represents a parsed mkpipe pipeline."""

    name: str
    source: str
    destination: str
    tag: str
    direction: str  # "ingestion" | "distribution"
    tables: list[MkpipeTable] = field(default_factory=list)


def _classify_direction(
    source_variant: str,
    destination_variant: str,
    destination_config: dict | None = None,
) -> str:
    """Detect pipeline direction from connection variants.

    - destination is a DWH target (snowflake/clickhouse) -> ingestion
    - destination is S3/Iceberg (file+iceberg) -> ingestion
    - source is a DWH source (snowflake/clickhouse) -> distribution
    """
    if destination_variant in DWH_TARGET_VARIANTS:
        return 'ingestion'

    # S3/Iceberg is also ingestion
    if destination_variant == 'file' and destination_config:
        extra = destination_config.get('extra', {})
        if extra.get('format') == 'iceberg' and extra.get('storage') == 's3':
            return 'ingestion'

    if source_variant in DWH_SOURCE_VARIANTS:
        return 'distribution'
    return 'other'


def parse_mkpipe_config(
    config_path: Path,
    environment: str | None = None,
) -> list[PipelineInfo]:
    """Parse mkpipe_project.yaml and return structured pipeline/table metadata.

    Args:
        config_path: Path to mkpipe_project.yaml
        environment: Environment key to use. If None, uses default_environment from yaml.

    Returns:
        List of PipelineInfo with classified direction and table-level tags.
    """
    with open(config_path) as f:
        raw = yaml.safe_load(f)

    if environment is None:
        environment = raw.get('default_environment', 'prod')

    env_config = raw.get(environment)
    if env_config is None:
        msg = f"Environment '{environment}' not found in {config_path}"
        raise ValueError(msg)

    connections: dict[str, dict] = env_config.get('connections', {})
    pipelines_raw: list[dict] = env_config.get('pipelines', [])

    result: list[PipelineInfo] = []

    for pipeline in pipelines_raw:
        pipeline_name = pipeline['name']
        source_conn = pipeline['source']
        dest_conn = pipeline['destination']
        pipeline_tag = pipeline.get('tag', '')

        source_variant = connections.get(source_conn, {}).get('variant', '')
        dest_variant = connections.get(dest_conn, {}).get('variant', '')
        dest_config = connections.get(dest_conn, {})
        direction = _classify_direction(source_variant, dest_variant, dest_config)

        tables: list[MkpipeTable] = []
        for table_raw in pipeline.get('tables', []):
            # Table-level tags override; fall back to pipeline-level tag
            table_tags = table_raw.get('tags', [])
            if not table_tags and pipeline_tag:
                table_tags = [pipeline_tag]

            tables.append(
                MkpipeTable(
                    name=table_raw['name'],
                    target_name=table_raw.get('target_name', table_raw['name']),
                    replication_method=table_raw.get('replication_method', 'full'),
                    pipeline_name=pipeline_name,
                    source_connection=source_conn,
                    destination_connection=dest_conn,
                    direction=direction,
                    tags=table_tags,
                    fetchsize=table_raw.get('fetchsize', 1000),
                    custom_sql=table_raw.get('custom_sql'),
                )
            )

        info = PipelineInfo(
            name=pipeline_name,
            source=source_conn,
            destination=dest_conn,
            tag=pipeline_tag,
            direction=direction,
            tables=tables,
        )
        result.append(info)

        logger.info(
            "Parsed pipeline '%s': direction=%s, tag=%s, tables=%d",
            pipeline_name,
            direction,
            pipeline_tag,
            len(tables),
        )

    return result


def get_tables_by_direction(
    pipelines: list[PipelineInfo],
    direction: str,
) -> list[MkpipeTable]:
    """Get all tables with a specific direction across all pipelines."""
    return [
        table
        for pipeline in pipelines
        if pipeline.direction == direction
        for table in pipeline.tables
    ]


def get_tables_by_tag(
    pipelines: list[PipelineInfo],
    tag: str,
) -> list[MkpipeTable]:
    """Get all tables that have a specific tag."""
    return [
        table
        for pipeline in pipelines
        for table in pipeline.tables
        if tag in table.tags
    ]


def get_all_tags(pipelines: list[PipelineInfo]) -> set[str]:
    """Extract all unique tags across all tables."""
    return {
        tag for pipeline in pipelines for table in pipeline.tables for tag in table.tags
    }
