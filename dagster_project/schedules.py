from __future__ import annotations

from typing import Any, Sequence

from dagster import ScheduleDefinition

from dagster_project.config.settings import EXPOSURE_GROUPS


def build_schedules(jobs: Sequence[Any]) -> list[ScheduleDefinition]:
    """Build cron schedules for each exposure-group job.

    Args:
        jobs: Pre-built exposure jobs from build_exposure_jobs().

    Schedule config is read from settings.EXPOSURE_GROUPS.
    Each group maps to a cron expression.
    """
    job_by_name = {job.name: job for job in jobs}

    schedules: list[ScheduleDefinition] = []

    for group_name, config in EXPOSURE_GROUPS.items():
        job_name = f'{group_name}_pipeline_job'
        job = job_by_name.get(job_name)
        if job is None:
            continue

        schedules.append(
            ScheduleDefinition(
                name=f'{group_name}_schedule',
                job=job,
                cron_schedule=config.cron,
            )
        )

    return schedules
