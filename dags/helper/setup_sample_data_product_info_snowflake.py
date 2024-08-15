"""
## Demo DAG: to load sample sneakers data to Snowflake

This DAG loads product info about sneakers to a Snowflake table.
It creates the table if it does not exist yet in the specified DB and schema.
"""

from airflow.decorators import dag, task
from airflow.datasets import Dataset
from airflow.models.baseoperator import chain
from airflow.io.path import ObjectStoragePath
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from pendulum import datetime, duration
import logging
import os

# Get the Airflow task logger
t_log = logging.getLogger("airflow.task")

# Snowflake variables
_SNOWLFLAKE_CONN_ID = os.getenv("SNOWFLAKE_CONN_ID", "snowflake_default")
_SNOWFLAKE_DB_NAME = os.getenv("SNOWFLAKE_DB_NAME")
_SNOWFLAKE_SCHEMA_NAME = os.getenv("SNOWFLAKE_SCHEMA_NAME")
_SNOWFLAKE_TABLE_NAME = os.getenv("SNOWFLAKE_TABLE_NAME_SNEAKERS_DATA")

# Creating ObjectStoragePath objects
# See https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/objectstorage.html
# for more information on the Airflow Object Storage feature
OBJECT_STORAGE_SRC = "file"
CONN_ID_SRC = None
KEY_SRC = "include/demo_data/sneakers_info/sneakers.json"

base_src = ObjectStoragePath(f"{OBJECT_STORAGE_SRC}://{KEY_SRC}", conn_id=CONN_ID_SRC)

# -------------- #
# DAG definition #
# -------------- #


@dag(
    dag_display_name="🛠️ Load sample product info to Snowflake",
    start_date=datetime(2024, 8, 1),
    schedule=[Dataset("setup")],
    catchup=False,
    default_args={
        "owner": "Demo team",
        "retries": 3,
        "retry_delay": duration(minutes=1),
    },
    doc_md=__doc__,
    description="Helper",
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
        """Insert product info data into snowflake."""

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

    # ------------------------------ #
    # Define additional dependencies #
    # ------------------------------ #

    chain(create_table_if_not_exists, insert_data_into_snowflake_obj, sneaker_data_in())


setup_sample_data_product_info_snowflake()
