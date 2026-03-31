from __future__ import annotations

import logging
from pathlib import Path

from dagster import ConfigurableResource

logger = logging.getLogger(__name__)


class MkpipeResource(ConfigurableResource):
    """Dagster resource wrapping mkpipe.run() for table-level execution."""

    config_path: str

    def run_table(self, table: str) -> None:
        """Run mkpipe for a single table.

        Args:
            table: The target_name of the table to extract/load.
        """
        import mkpipe

        config = str(Path(self.config_path).resolve())
        logger.info("Running mkpipe for table='%s' with config='%s'", table, config)
        mkpipe.run(config=config, table=table)
        logger.info("Completed mkpipe for table='%s'", table)

    def run_tags(self, tags: list[str]) -> None:
        """Run mkpipe for all tables matching the given tags.

        This runs matching tables across ALL pipelines in a single Spark session.

        Args:
            tags: List of tags to filter tables by.
        """
        import mkpipe

        config = str(Path(self.config_path).resolve())
        logger.info("Running mkpipe for tags=%s with config='%s'", tags, config)
        mkpipe.run(config=config, tags=tags)
        logger.info("Completed mkpipe for tags=%s", tags)

    def run_pipeline(self, pipeline: str) -> None:
        """Run mkpipe for an entire pipeline.

        Args:
            pipeline: The pipeline name to run.
        """
        import mkpipe

        config = str(Path(self.config_path).resolve())
        logger.info("Running mkpipe for pipeline='%s' with config='%s'", pipeline, config)
        mkpipe.run(config=config, pipeline=pipeline)
        logger.info("Completed mkpipe for pipeline='%s'", pipeline)
