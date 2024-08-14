"""
### Demo DAG: to load sample images to S3 from local storage

To use a different remote storage option replace the S3CreateBucketOperator,
as well as change the OBJECT_STORAGE_DST, CONN_ID_DST and KEY_DST
parameters.
"""

from airflow.decorators import dag, task
from airflow.datasets import Dataset
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.io.path import ObjectStoragePath
from pendulum import datetime, duration
from airflow.models.baseoperator import chain
import os
import logging

# Get the Airflow task logger
t_log = logging.getLogger("airflow.task")

# S3 variables
_AWS_CONN_ID = os.getenv("AWS_CONN_ID")
_S3_BUCKET = os.getenv("S3_BUCKET")
_INGEST_FOLDER_NAME = os.getenv("INGEST_FOLDER_NAME")
_IMAGE_FOLDER_NAME = os.getenv("IMAGES_FOLDER_NAME")

# Creating ObjectStoragePath objects
# See https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/objectstorage.html
# for more information on the Airflow Object Storage feature
OBJECT_STORAGE_SRC = "file"
CONN_ID_SRC = None
KEY_SRC = "include/demo_data/images/"

OBJECT_STORAGE_DST = "s3"
CONN_ID_DST = _AWS_CONN_ID
KEY_DST = _S3_BUCKET + "/" + _INGEST_FOLDER_NAME

base_src = ObjectStoragePath(f"{OBJECT_STORAGE_SRC}://{KEY_SRC}", conn_id=CONN_ID_SRC)
base_dst = ObjectStoragePath(f"{OBJECT_STORAGE_DST}://{KEY_DST}", conn_id=CONN_ID_DST)

# -------------- #
# DAG definition #
# -------------- #


@dag(
    dag_display_name="🛠️ Load sample images to S3",
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
def setup_sample_data_img():

    create_bucket = S3CreateBucketOperator(
        task_id="create_bucket_if_not_exists",
        aws_conn_id=_AWS_CONN_ID,
        bucket_name=_S3_BUCKET,
    )

    @task
    def list_files_sample_img(
        path_src: ObjectStoragePath,
    ) -> list[ObjectStoragePath]:
        """List files in local object storage."""
        t_log.info(f"Checking for folders at: {path_src.as_uri()}")
        files = [f for f in path_src.iterdir() if f.is_file()]
        return files

    list_files_sample_img_obj = list_files_sample_img(path_src=base_src)

    @task(map_index_template="{{ my_custom_map_index }}")
    def copy_local_to_remote(file: ObjectStoragePath, base_dst: ObjectStoragePath):
        """Copy files from local storage to remote object storage."""

        full_key = base_dst / os.path.join(*file.parts[-2:])
        file.copy(dst=full_key)
        t_log.info(f"Successfully wrote {full_key} to remote storage!")

        # get the current context and define the custom map index variable
        from airflow.operators.python import get_current_context

        context = get_current_context()
        context["my_custom_map_index"] = f"Copying: {file.as_uri()}"

    @task(
        outlets=[
            Dataset(base_dst.as_uri() + f"/{_IMAGE_FOLDER_NAME}"),
        ]
    )
    def sample_img_in():
        t_log.info("Sample images loaded to remote storage!")

    # ------------------------------ #
    # Define additional dependencies #
    # ------------------------------ #

    chain(
        [create_bucket, list_files_sample_img_obj],
        copy_local_to_remote.partial(base_dst=base_dst).expand(
            file=list_files_sample_img_obj
        ),
        sample_img_in(),
    )


setup_sample_data_img()
