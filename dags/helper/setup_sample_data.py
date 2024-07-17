"""
## Demo DAG to set up S3 with the sample data in include/sample_data

To use a different remote storage option replace the S3CreateBucketOperator,
as well as change the OBJECT_STORAGE_DST, CONN_ID_DST and KEY_DST
parameters.
"""

from airflow.decorators import dag, task
from airflow.datasets import Dataset
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.io.path import ObjectStoragePath
from pendulum import datetime
from airflow.models.baseoperator import chain
import os
import logging

t_log = logging.getLogger("airflow.task")

_AWS_CONN_ID = os.getenv("AWS_CONN_ID")
_S3_BUCKET = os.getenv("S3_BUCKET")
_INGEST_FOLDER_NAME = os.getenv("INGEST_FOLDER_NAME")
_TYPE_FOLDER_NAME_1 = os.getenv("IMAGE_FOLDER_NAME")
_TYPE_FOLDER_NAME_2 = os.getenv("TEXT_FOLDER_NAME")

OBJECT_STORAGE_SRC = "file"
CONN_ID_SRC = None
KEY_SRC = "include/sample_data"

OBJECT_STORAGE_DST = "s3"
CONN_ID_DST = _AWS_CONN_ID
KEY_DST = _S3_BUCKET + "/" + _INGEST_FOLDER_NAME

base_src = ObjectStoragePath(f"{OBJECT_STORAGE_SRC}://{KEY_SRC}", conn_id=CONN_ID_SRC)
base_dst = ObjectStoragePath(f"{OBJECT_STORAGE_DST}://{KEY_DST}", conn_id=CONN_ID_DST)


@dag(
    dag_display_name="Load sample data to S3",
    start_date=datetime(2024, 7, 1),
    schedule="@daily",
    catchup=False,
    tags=["demo"],
)
def setup_sample_data():

    create_bucket = S3CreateBucketOperator(
        task_id="create_bucket", aws_conn_id=_AWS_CONN_ID, bucket_name=_S3_BUCKET
    )

    @task
    def list_folders_sample_data(
        path_src: ObjectStoragePath,
    ) -> list[ObjectStoragePath]:
        """List files in local object storage."""
        folders = [f for f in path_src.iterdir() if f.is_dir()]
        return folders

    folders_sample_data = list_folders_sample_data(path_src=base_src)

    @task
    def copy_local_to_remote(path_src: ObjectStoragePath, base_dst: ObjectStoragePath):
        """Copy files from local storage to remote object storage."""

        for folder in path_src.iterdir():
            for f in folder.iterdir():
                full_key = base_dst / os.path.join(*f.parts[-3:])
                f.copy(dst=full_key)
                t_log.info(f"Successfully wrote {full_key} to remote storage!")

    @task(
        outlets=[
            Dataset(base_dst.as_uri() + "/" + _TYPE_FOLDER_NAME_1),
            Dataset(base_dst.as_uri() + "/" + _TYPE_FOLDER_NAME_2),
        ]
    )
    def sample_data_in():
        t_log.info("Sample data loaded to remote storage!")

    chain(
        [create_bucket, folders_sample_data],
        copy_local_to_remote.partial(base_dst=base_dst).expand(
            path_src=folders_sample_data
        ),
        sample_data_in(),
    )


setup_sample_data()
