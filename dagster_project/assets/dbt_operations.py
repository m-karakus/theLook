import logging
import subprocess

from dagster import AssetKey, asset

from dagster_project.config.settings import DBT_PROJECT_DIR

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
def create_iceberg_tables() -> None:
    """Create missing Iceberg tables in Snowflake from Glue Catalog.

    This is an idempotent operation — existing tables are skipped.
    Only needs to run when new raw tables are added to sources.yml.
    """
    logger.info('Running dbt run-operation create_iceberg_tables')
    _run_dbt_operation('create_iceberg_tables')
    logger.info('create_iceberg_tables completed')


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
def refresh_all_iceberg_tables() -> None:
    """Refresh all Iceberg tables to sync with latest S3/Glue metadata.

    Individual model builds already refresh their own source via pre-hook.
    This asset is for bulk refresh when needed.
    """
    logger.info('Running dbt run-operation refresh_iceberg_tables')
    _run_dbt_operation('refresh_iceberg_tables')
    logger.info('refresh_iceberg_tables completed')


def _run_dbt_operation(operation_name: str) -> None:
    """Run a dbt run-operation command as a subprocess."""
    cmd = [
        'dbt', 'run-operation', operation_name,
        '--project-dir', str(DBT_PROJECT_DIR),
        '--profiles-dir', str(DBT_PROJECT_DIR / '.dbt'),
    ]
    logger.info('Executing: %s', ' '.join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)

    if result.stdout:
        logger.info(result.stdout)
    if result.stderr:
        logger.warning(result.stderr)

    if result.returncode != 0:
        raise RuntimeError(
            f'dbt run-operation {operation_name} failed with exit code {result.returncode}:\n'
            f'{result.stderr}'
        )
