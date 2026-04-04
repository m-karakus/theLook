import logging

from dagster import AssetKey, OpExecutionContext, asset
from dagster_dbt import DbtCliResource

logger = logging.getLogger(__name__)


@asset(
    key=AssetKey(['ops', 'create_iceberg_tables']),
    group_name='dbt_operations',
    description=(
        'Creates Iceberg external tables in Snowflake for all raw_* sources. '
        'Runs `dbt run-operation create_iceberg_tables`. '
        'Trigger manually from the Dagster UI when adding new pipelines.'
    ),
    kinds={'snowflake', 'dbt'},
)
def create_iceberg_tables(context: OpExecutionContext, dbt: DbtCliResource) -> None:
    """Create missing Iceberg tables in Snowflake from Glue Catalog.

    This is an idempotent operation — existing tables are skipped.
    Only needs to run when new raw tables are added to sources.yml.
    """
    logger.info('Running dbt run-operation create_iceberg_tables')
    result = dbt.cli(['run-operation', 'create_iceberg_tables'], context=context)
    logger.info('create_iceberg_tables completed: %s', result)


@asset(
    key=AssetKey(['ops', 'refresh_all_iceberg_tables']),
    group_name='dbt_operations',
    description=(
        'Refreshes ALL Iceberg external tables in Snowflake. '
        'Runs `dbt run-operation refresh_iceberg_tables`. '
        'Use when you need a full refresh outside of normal dbt builds.'
    ),
    kinds={'snowflake', 'dbt'},
)
def refresh_all_iceberg_tables(context: OpExecutionContext, dbt: DbtCliResource) -> None:
    """Refresh all Iceberg tables to sync with latest S3/Glue metadata.

    Individual model builds already refresh their own source via pre-hook.
    This asset is for bulk refresh when needed.
    """
    logger.info('Running dbt run-operation refresh_iceberg_tables')
    result = dbt.cli(['run-operation', 'refresh_iceberg_tables'], context=context)
    logger.info('refresh_iceberg_tables completed: %s', result)
