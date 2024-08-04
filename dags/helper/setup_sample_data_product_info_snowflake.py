"""
## Demo DAG to load sample sneakers data to Snowflake

"""

from airflow.decorators import dag, task
from airflow.datasets import Dataset
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.io.path import ObjectStoragePath
from pendulum import datetime
from airflow.models.baseoperator import chain
import os
import logging

t_log = logging.getLogger("airflow.task")

OBJECT_STORAGE_SRC = "file"
CONN_ID_SRC = None
KEY_SRC = "include/demo_data/sneakers_info/sneakers.json"

base_src = ObjectStoragePath(f"{OBJECT_STORAGE_SRC}://{KEY_SRC}", conn_id=CONN_ID_SRC)

_SNOWLFLAKE_CONN_ID = os.getenv("SNOWFLAKE_CONN_ID", "snowflake_default")
_SNOWFLAKE_DB_NAME = "hybrid_search_demo"
_SNOWFLAKE_SCHEMA_NAME = "dev"
_SNOWFLAKE_TABLE_NAME = "sneakers_data"


@dag(
    dag_display_name="Load sample product info to Snowflake",
    start_date=datetime(2024, 7, 1),
    schedule=[Dataset("setup")],
    catchup=False,
    tags=["helper"],
)
def setup_sample_data_product_info_snowflake():

    @task
    def extract_data_from_json(
        path_src: ObjectStoragePath,
    ) -> list[ObjectStoragePath]:
        """List files in local object storage."""
        import json

        with open(path_src.path, "r") as file:
            data = json.load(file)
        return data

    extract_data_from_json_obj = extract_data_from_json(path_src=base_src)

    create_table_if_not_exists = SQLExecuteQueryOperator(
        task_id="create_table_if_not_exists",
        conn_id=_SNOWLFLAKE_CONN_ID,
        sql=f"""
                CREATE TABLE IF NOT EXISTS 
                {_SNOWFLAKE_DB_NAME}.{_SNOWFLAKE_SCHEMA_NAME}.{_SNOWFLAKE_TABLE_NAME} (
                    uuid STRING PRIMARY KEY,
                    title STRING,
                    description STRING,
                    price FLOAT,
                    category STRING,
                    file_path STRING
                );
            """,
        show_return_value_in_logs=True,
    )

    @task(map_index_template="{{ my_custom_map_index }}")
    def insert_data_into_snowflake(sneakers_data):
        insert_sql = f"""
        INSERT INTO {_SNOWFLAKE_DB_NAME}.{_SNOWFLAKE_SCHEMA_NAME}.{_SNOWFLAKE_TABLE_NAME} (
            uuid, title, description, price, category, file_path
        ) VALUES (
            %(uuid)s, %(title)s, %(description)s, %(price)s, %(category)s, %(file_path)s
        );
        """
        snowflake_hook = SnowflakeHook(snowflake_conn_id=_SNOWLFLAKE_CONN_ID)
        snowflake_hook.run(insert_sql, parameters=sneakers_data)

        # get the current context and define the custom map index variable
        from airflow.operators.python import get_current_context

        context = get_current_context()
        context["my_custom_map_index"] = f"Inserting info on: {sneakers_data['title']}"

    insert_data_into_snowflake_obj = insert_data_into_snowflake.expand(
        sneakers_data=extract_data_from_json_obj
    )

    @task(
        outlets=[
            Dataset(
                f"snowflake://{_SNOWFLAKE_DB_NAME}.{_SNOWFLAKE_SCHEMA_NAME}.{_SNOWFLAKE_TABLE_NAME}"
            ),
        ]
    )
    def sneaker_data_in():
        t_log.info("Sample data loaded to Snowflake!")

    chain(create_table_if_not_exists, insert_data_into_snowflake_obj, sneaker_data_in())


setup_sample_data_product_info_snowflake()
