from setuptools import find_packages, setup

setup(
    name='dagster_project',
    packages=find_packages(),
    install_requires=[
        'dagster>=1.9',
        'dagster-webserver>=1.9',
        'dagster-dbt>=0.25',
        'dbt-core>=1.10,<2',
        'dbt-snowflake>=1.10,<2',
    ],
    extras_require={
        'dev': [
            'pytest',
        ]
    },
)
