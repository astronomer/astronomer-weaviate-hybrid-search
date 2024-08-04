from airflow.decorators import dag, task
from airflow.datasets import Dataset
from airflow.timetables.datasets import DatasetOrTimeSchedule
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.io.path import ObjectStoragePath
from airflow.models.baseoperator import chain
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from pendulum import datetime
import os
import logging
import json

from include.functions.utils import get_all_files, get_all_checksums, compare_checksums

t_log = logging.getLogger("airflow.task")

_AWS_CONN_ID = os.getenv("AWS_CONN_ID")
_S3_BUCKET = os.getenv("S3_BUCKET")
_STAGE_FOLDER_NAME = os.getenv("STAGE_FOLDER_NAME")
_PRODUCT_INFO_FOLDER_NAME = "product_info"

_SNOWLFLAKE_CONN_ID = os.getenv("SNOWFLAKE_CONN_ID", "snowflake_default")
_SNOWFLAKE_DB_NAME = "hybrid_search_demo"
_SNOWFLAKE_SCHEMA_NAME = "dev"
_SNOWFLAKE_TABLE_NAME = "sneakers_data"


OBJECT_STORAGE_DST = "s3"
CONN_ID_DST = _AWS_CONN_ID
KEY_DST = _S3_BUCKET + "/" + _STAGE_FOLDER_NAME + "/" + _PRODUCT_INFO_FOLDER_NAME + "/" + _SNOWFLAKE_TABLE_NAME


BASE_DST = ObjectStoragePath(f"{OBJECT_STORAGE_DST}://{KEY_DST}", conn_id=CONN_ID_DST)


@dag(
    dag_display_name="📝 Product Info Ingestion from ❄️",
    start_date=datetime(2024, 7, 1),
    schedule=DatasetOrTimeSchedule(
        timetable=CronTriggerTimetable("0 0 1 * *", timezone="UTC"),
        datasets=[
            Dataset(
                f"snowflake://{_SNOWFLAKE_DB_NAME}.{_SNOWFLAKE_SCHEMA_NAME}.{_SNOWFLAKE_TABLE_NAME}"
            ),
        ],
    ),
    catchup=False,
    tags=["ingest"],
)
def in_product_info_snowflake():

    query_sneakers_data = SQLExecuteQueryOperator(
        task_id="query_sneakers_data",
        conn_id=_SNOWLFLAKE_CONN_ID,
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
        sneakers_data = json.loads(data[0][0])
        print(sneakers_data)
        return sneakers_data

    @task(
        outlets=Dataset(BASE_DST.as_uri())
    )
    def load_to_s3(data: dict, base_dst: ObjectStoragePath) -> None:
        """Copy a file from remote to local storage.
        The file is streamed in chunks using shutil.copyobj"""

        file_name = base_dst / "sneakers.json"
        file_name.write_bytes(json.dumps(data).encode('utf-8'))


    transform_output_obj = transform_output(data=query_sneakers_data.output)

    load_to_s3_obj = load_to_s3(base_dst=BASE_DST, data=transform_output_obj)

    chain(query_sneakers_data, transform_output_obj)


in_product_info_snowflake()
