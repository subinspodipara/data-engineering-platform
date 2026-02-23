from airflow import DAG
from datetime import datetime

from cosmos import (
    DbtTaskGroup,
    ProjectConfig,
    ProfileConfig,
    ExecutionConfig,
    RenderConfig,
    LoadMode,
)
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

with DAG(
    dag_id="cosmos_dbt_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
):

    dbt_models = DbtTaskGroup(
        group_id="dbt_models",

project_config=ProjectConfig(
    "/Users/subinssubins/data_engineering/my_dbt_project",
    manifest_path="/Users/subinssubins/data_engineering/my_dbt_project/target/manifest.json",
),

        profile_config=ProfileConfig(
            profile_name="my_dbt_project",
            target_name="dev",
            profile_mapping=SnowflakeUserPasswordProfileMapping(
                conn_id="snowflake_default",
                profile_args={"schema": "DBT_DEV"},
            ),
        ),

        execution_config=ExecutionConfig(
            dbt_executable_path="/Users/subinssubins/data_engineering/dbt_env/bin/dbt"
        ),

        render_config=RenderConfig(
            load_method=LoadMode.DBT_MANIFEST
        ),
    )