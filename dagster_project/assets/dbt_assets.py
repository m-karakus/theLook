import os
from typing import Any, Mapping, Optional

from dagster import AssetExecutionContext, AssetKey
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

from dagster_project.config.settings import DBT_PROJECT_DIR

# DbtProject handles manifest generation:
# - In development (dagster dev): runs `dbt parse` automatically if manifest is missing
# - In deployment: uses pre-built manifest from `dbt_project/target/manifest.json`
dbt_project = DbtProject(
    project_dir=os.fspath(DBT_PROJECT_DIR),
    profiles_dir=os.fspath(DBT_PROJECT_DIR / '.dbt'),
)
dbt_project.prepare_if_dev()


class TheLookDbtTranslator(DagsterDbtTranslator):
    """Custom translator to align dbt asset keys with mkpipe ingestion asset keys.

    - dbt sources (raw.*) -> AssetKey(["ingestion", table_name])
      This creates the link between mkpipe ingestion assets and dbt models.
    - dbt models -> AssetKey(["dwh", model_name])
    """

    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> AssetKey:
        resource_type = dbt_resource_props.get('resource_type', '')

        if resource_type == 'source':
            source_name = dbt_resource_props.get('source_name', '')
            table_name = dbt_resource_props.get('name', '')

            # raw sources -> map to mkpipe ingestion asset keys
            # e.g. source('raw', 'raw_bq__orders') -> AssetKey(["ingestion", "raw_bq__orders"])
            if source_name == 'raw':
                return AssetKey(['ingestion', table_name])

            # Other sources: use source_name as prefix
            return AssetKey([source_name, table_name])

        # dbt models -> AssetKey(["dwh", model_name])
        model_name = dbt_resource_props.get('name', '')
        return AssetKey(['dwh', model_name])

    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> Optional[str]:
        resource_type = dbt_resource_props.get('resource_type', '')
        if resource_type == 'source':
            return None

        # Group dbt models by their folder path
        fqn = dbt_resource_props.get('fqn', [])
        # fqn example: ["dbt_project", "thelook", "mart", "mart_customer_revenue"]
        if len(fqn) >= 3:
            return f'dbt_{fqn[2]}'
        return 'dbt_models'

    def get_tags(self, dbt_resource_props: Mapping[str, Any]) -> dict[str, str]:
        """Convert dbt tags to Dagster tags.

        Only operational tags (like 'heavy') are propagated.
        """
        tags = dict(super().get_tags(dbt_resource_props))
        dbt_tags = dbt_resource_props.get('tags', [])
        operational_tags = {'heavy'}

        for dbt_tag in dbt_tags:
            if dbt_tag in operational_tags:
                tags[f'dbt.{dbt_tag}'] = 'true'

        return tags


@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=TheLookDbtTranslator(),
    project=dbt_project,
)
def dwh_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """All dbt models auto-discovered from manifest.json.

    Adding a new dbt model + running `dbt parse` will automatically
    create a new Dagster asset with correct upstream/downstream links.
    """
    yield from dbt.cli(['build'], context=context).stream()
