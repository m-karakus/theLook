"""Credentials Update Script -- Single Source of Truth.

Reads .credentials.yaml and updates project config files.

Credentials schema (per environment):
  mkpipe:    settings + connections blocks -> merged into mkpipe_project.yaml
             (pipelines section is NEVER touched)
  dbt:       full profiles.yml content -> written verbatim
  dagster:   top-level keys -> merged into dagster.yaml
             (run_launcher, compute_logs, python_logs etc. are preserved)

Usage:
    python .credentials_update.py --env dev
    python .credentials_update.py --env dev --dry-run
"""

from __future__ import annotations

import argparse
import copy
import logging
import sys
from pathlib import Path

import yaml

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

ROOT_DIR = Path(__file__).resolve().parent

CREDENTIALS_FILE = ROOT_DIR / '.credentials.yaml'
MKPIPE_YAML = ROOT_DIR / 'extract_load_project' / 'mkpipe_project.yaml'
DBT_PROFILES = ROOT_DIR / 'dbt_project' / '.dbt' / 'profiles.yml'
DAGSTER_YAML = ROOT_DIR / 'dagster.yaml'


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _read_yaml(path: Path) -> dict:
    with open(path) as f:
        return yaml.safe_load(f) or {}


def _write_yaml(path: Path, data: dict, dry_run: bool) -> None:
    if dry_run:
        logger.info('  [dry-run] %s NOT written', path)
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w') as f:
        yaml.safe_dump(data, f, default_flow_style=False, sort_keys=False)
    logger.info('  Written: %s', path)


# ---------------------------------------------------------------------------
# Load + validate
# ---------------------------------------------------------------------------


def load_credentials(env: str) -> dict:
    """Load credentials for the given environment."""
    if not CREDENTIALS_FILE.exists():
        logger.error(
            'Credentials file not found: %s\n'
            'Copy .credentials.yaml.example to .credentials.yaml and fill in values.',
            CREDENTIALS_FILE,
        )
        sys.exit(1)

    all_creds = _read_yaml(CREDENTIALS_FILE)

    if not isinstance(all_creds, dict):
        logger.error('Credentials file is empty or malformed: %s', CREDENTIALS_FILE)
        sys.exit(1)

    if env not in all_creds:
        logger.error(
            "Environment '%s' not found in %s.\nAvailable: %s",
            env,
            CREDENTIALS_FILE,
            list(all_creds.keys()),
        )
        sys.exit(1)

    creds = all_creds[env]

    has_any = any(key in creds for key in ('mkpipe', 'dbt', 'dagster'))
    if not has_any:
        logger.error(
            'credentials[%s] has none of: mkpipe, dbt, dagster. Nothing to do.',
            env,
        )
        sys.exit(1)

    return creds


# ---------------------------------------------------------------------------
# Step 1: mkpipe -- merge settings + connections, preserve pipelines
# ---------------------------------------------------------------------------

_MKPIPE_PIPELINE_KEYS = frozenset({'pipelines'})


def update_mkpipe(creds: dict, env: str, dry_run: bool) -> None:
    """Merge mkpipe credentials into mkpipe_project.yaml."""
    mkpipe_creds = creds.get('mkpipe')
    if not mkpipe_creds:
        logger.info('  No mkpipe block in credentials, skipping')
        return

    if not MKPIPE_YAML.exists():
        logger.warning('  mkpipe_project.yaml not found: %s', MKPIPE_YAML)
        return

    config = _read_yaml(MKPIPE_YAML)
    mkpipe_env = config.get('default_environment', env)

    if mkpipe_env not in config:
        available = [k for k in config if k not in ('version', 'default_environment')]
        logger.warning(
            "  Environment '%s' not in mkpipe yaml. Available: %s",
            mkpipe_env,
            available,
        )
        return

    env_block = config[mkpipe_env]

    replaced: list[str] = []
    for key, value in mkpipe_creds.items():
        if key in _MKPIPE_PIPELINE_KEYS:
            logger.warning("  Ignoring '%s' in credentials.mkpipe (protected)", key)
            continue
        env_block[key] = copy.deepcopy(value)
        replaced.append(key)

    if replaced:
        logger.info('  Replaced keys in mkpipe[%s]: %s', mkpipe_env, replaced)
    else:
        logger.info('  No keys to replace in mkpipe')
        return

    _write_yaml(MKPIPE_YAML, config, dry_run)


# ---------------------------------------------------------------------------
# Step 2: dbt -- write profiles.yml verbatim
# ---------------------------------------------------------------------------


def update_dbt(creds: dict, dry_run: bool) -> None:
    """Write dbt profiles.yml entirely from the credentials dbt block."""
    dbt_creds = creds.get('dbt')
    if not dbt_creds:
        logger.info('  No dbt block in credentials, skipping')
        return

    for profile_name, profile_cfg in dbt_creds.items():
        target = profile_cfg.get('target', '?')
        outputs = list(profile_cfg.get('outputs', {}).keys())
        logger.info(
            "  Profile '%s': target=%s, outputs=%s", profile_name, target, outputs
        )

    _write_yaml(DBT_PROFILES, copy.deepcopy(dbt_creds), dry_run)


# ---------------------------------------------------------------------------
# Step 3: dagster -- merge top-level keys into dagster.yaml
# ---------------------------------------------------------------------------


def update_dagster(creds: dict, dry_run: bool) -> None:
    """Merge dagster credentials into dagster.yaml."""
    dagster_creds = creds.get('dagster')
    if not dagster_creds:
        logger.info('  No dagster block in credentials, skipping')
        return

    if not DAGSTER_YAML.exists():
        logger.warning('  dagster.yaml not found: %s', DAGSTER_YAML)
        return

    config = _read_yaml(DAGSTER_YAML)

    replaced: list[str] = []
    for key, value in dagster_creds.items():
        config[key] = copy.deepcopy(value)
        replaced.append(key)

    logger.info('  Replaced keys in dagster.yaml: %s', replaced)

    _write_yaml(DAGSTER_YAML, config, dry_run)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Update project configs from .credentials.yaml',
    )
    parser.add_argument(
        '--env',
        required=True,
        help='Target environment (e.g. dev)',
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would change without writing files',
    )
    args = parser.parse_args()

    if args.dry_run:
        logger.info('=== DRY RUN ===')

    logger.info('Loading credentials for: %s', args.env)
    creds = load_credentials(args.env)

    logger.info('STEP 1: mkpipe_project.yaml (settings + connections)...')
    update_mkpipe(creds, args.env, dry_run=args.dry_run)

    logger.info('STEP 2: dbt profiles.yml...')
    update_dbt(creds, dry_run=args.dry_run)

    logger.info('STEP 3: dagster.yaml...')
    update_dagster(creds, dry_run=args.dry_run)

    if args.dry_run:
        logger.info('=== DRY RUN complete ===')
    else:
        logger.info('Done! All configs updated for: %s', args.env)


if __name__ == '__main__':
    main()
