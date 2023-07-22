from dagster import file_relative_path
from dagster_dbt import load_assets_from_dbt_project

DBT_PROJECT_DIR = file_relative_path(__file__, "../peerdb_demo_dbt")

dbt_assets = load_assets_from_dbt_project(
    project_dir=DBT_PROJECT_DIR,
    profiles_dir=DBT_PROJECT_DIR,
    key_prefix=[
        "peerdb_transforms",
    ],
)
