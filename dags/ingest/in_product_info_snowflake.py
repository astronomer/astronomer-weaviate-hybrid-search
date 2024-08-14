"""
### ETL: Ingest product information from Snowflake to S3

This DAG moves product information about sneakers from Snowflake
to S3 to be loaded into Weaviate with the next DAG.
"""

from airflow.decorators import dag, task
from airflow.datasets import Dataset
from airflow.timetables.datasets import DatasetOrTimeSchedule
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.io.path import ObjectStoragePath
from airflow.models.baseoperator import chain
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from pendulum import datetime, duration
import logging
import json
import os

# Get the Airflow task logger
t_log = logging.getLogger("airflow.task")

# S3 variables
_AWS_CONN_ID = os.getenv("AWS_CONN_ID")
_S3_BUCKET = os.getenv("S3_BUCKET")
_STAGE_FOLDER_NAME = os.getenv("STAGE_FOLDER_NAME")
_PRODUCT_INFO_FOLDER_NAME = os.getenv("PRODUCT_INFO_FOLDER_NAME")

# Snowflake variables
_SNOWFLAKE_CONN_ID = os.getenv("SNOWFLAKE_CONN_ID", "snowflake_default")
_SNOWFLAKE_DB_NAME = os.getenv("SNOWFLAKE_DB_NAME")
_SNOWFLAKE_SCHEMA_NAME = os.getenv("SNOWFLAKE_SCHEMA_NAME")
_SNOWFLAKE_TABLE_NAME = os.getenv("SNOWFLAKE_TABLE_NAME_SNEAKERS_DATA")

# Creating ObjectStoragePath objects
# See https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/objectstorage.html
# for more information on the Airflow Object Storage feature
OBJECT_STORAGE_DST = "s3"
CONN_ID_DST = _AWS_CONN_ID
KEY_DST = (
    _S3_BUCKET
    + "/"
    + _STAGE_FOLDER_NAME
    + "/"
    + _PRODUCT_INFO_FOLDER_NAME
    + "/"
    + _SNOWFLAKE_TABLE_NAME
)


BASE_DST = ObjectStoragePath(f"{OBJECT_STORAGE_DST}://{KEY_DST}", conn_id=CONN_ID_DST)

# -------------- #
# DAG definition #
# -------------- #


@dag(
    dag_display_name="📝 Ingest product information: ❄️ -> S3",
    start_date=datetime(2024, 8, 1),
    schedule=DatasetOrTimeSchedule(
        timetable=CronTriggerTimetable(
            "0 0 1 * *", timezone="UTC"
        ),  # This DAG runs once a  month plus whenever the dataset is updated
        datasets=[
            Dataset(
                f"snowflake://{_SNOWFLAKE_DB_NAME}.{_SNOWFLAKE_SCHEMA_NAME}.{_SNOWFLAKE_TABLE_NAME}"
            ),
        ],
    ),
    catchup=False,
    default_args={
        "owner": "DE team",
        "retries": 3,
        "retry_delay": duration(minutes=1),
    },
    doc_md=__doc__,
    description="ETL",
    tags=["ETL", "use-case"],
)
def in_product_info_snowflake():

    query_sneakers_data = SQLExecuteQueryOperator(
        task_id="query_sneakers_data",
        conn_id=_SNOWFLAKE_CONN_ID,
        sql=f"""
                SELECT
                    ARRAY_AGG(OBJECT_CONSTRUCT(
                        'uuid', uuid,
                        'title', title,
                        'description', description,
                        'price', price,
                        'category', category,
                        'file_path', file_path
                    )) AS data
                FROM {_SNOWFLAKE_DB_NAME}.{_SNOWFLAKE_SCHEMA_NAME}.{_SNOWFLAKE_TABLE_NAME};
            """,
        show_return_value_in_logs=True,
    )

    @task
    def transform_output(data):
        """
        Transform output form the SQLExecuteQueryOperator for loading to S3.
        """
        sneakers_data = json.loads(data[0][0])
        print(sneakers_data)
        return sneakers_data

    @task(outlets=Dataset(BASE_DST.as_uri()))
    def load_to_s3(data: dict, base_dst: ObjectStoragePath) -> None:
        """
        Write Sneakers data to S3.
        """
        file_name = base_dst / "sneakers.json"
        file_name.write_bytes(json.dumps(data).encode("utf-8"))

    transform_output_obj = transform_output(data=query_sneakers_data.output)

    load_to_s3(base_dst=BASE_DST, data=transform_output_obj)

    # ------------------------------ #
    # Define additional dependencies #
    # ------------------------------ #

    chain(query_sneakers_data, transform_output_obj)


in_product_info_snowflake()
