"""
### ETL: Ingest images from the ingestion S3 location to the stage S3 location

This DAG moves images from an ingestion location in S3 to
a staging location for the frontend to access.
"""

from airflow.decorators import dag, task
from airflow.datasets import Dataset
from airflow.timetables.datasets import DatasetOrTimeSchedule
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.io.path import ObjectStoragePath
from airflow.models.baseoperator import chain
from pendulum import datetime, duration
import logging
import os

# import modularized functions from the include folder
from include.functions.utils import get_all_files, get_all_checksums, compare_checksums

# Get the Airflow task logger
t_log = logging.getLogger("airflow.task")

# S3 variables
_AWS_CONN_ID = os.getenv("AWS_CONN_ID")
_S3_BUCKET = os.getenv("S3_BUCKET")
_INGEST_FOLDER_NAME = os.getenv("INGEST_FOLDER_NAME")
_STAGE_FOLDER_NAME = os.getenv("STAGE_FOLDER_NAME")
_IMAGE_FOLDER_NAME = os.getenv("IMAGES_FOLDER_NAME")


# Creating ObjectStoragePath objects
# See https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/objectstorage.html
# for more information on the Airflow Object Storage feature
OBJECT_STORAGE_SRC = "s3"
CONN_ID_SRC = _AWS_CONN_ID
KEY_SRC = _S3_BUCKET + "/" + _INGEST_FOLDER_NAME

OBJECT_STORAGE_DST = "s3"
CONN_ID_DST = _AWS_CONN_ID
KEY_DST = _S3_BUCKET + "/" + _STAGE_FOLDER_NAME

BASE_SRC = ObjectStoragePath(f"{OBJECT_STORAGE_SRC}://{KEY_SRC}", conn_id=CONN_ID_SRC)
BASE_DST = ObjectStoragePath(f"{OBJECT_STORAGE_DST}://{KEY_DST}", conn_id=CONN_ID_DST)

# -------------- #
# DAG definition #
# -------------- #


@dag(
    dag_display_name="🏞️ Ingest images S3 -> S3",
    start_date=datetime(2024, 8, 1),
    schedule=DatasetOrTimeSchedule(  # This DAG runs once at midnight, plus whenever a dataset is updated
        timetable=CronTriggerTimetable("0 0 * * *", timezone="UTC"),
        datasets=[Dataset(BASE_SRC.as_uri() + "/" + _IMAGE_FOLDER_NAME)],
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
def in_images():

    @task
    def list_ingest_files(
        base_path: ObjectStoragePath, image_folder: str
    ) -> list[ObjectStoragePath] | list:
        """List files in remote object storage."""
        path = base_path / image_folder

        if path.exists():
            files = [f for f in path.iterdir() if f.is_file()]
            return files
        else:
            return []

    @task(map_index_template="{{ my_custom_map_index }}")
    def copy_ingest_to_stage(
        file: ObjectStoragePath, base_dst: ObjectStoragePath
    ) -> None:
        """Copy a file from remote to local storage.
        The file is streamed in chunks using shutil.copyobj"""

        full_key = base_dst / os.path.join(*file.parts[-2:])
        file.copy(dst=full_key)

        # get the current context and define the custom map index variable
        from airflow.operators.python import get_current_context

        context = get_current_context()
        context["my_custom_map_index"] = f"Copying: {file.as_uri()}"

    @task(outlets=Dataset(BASE_DST.as_uri() + "/" + _IMAGE_FOLDER_NAME))
    def verify_checksum(
        base_src: ObjectStoragePath,
        base_dst: ObjectStoragePath,
        type_folder_name: str,
        folder_name_src: str,
        folder_name_dst: str,
    ):
        """Compares checksums to verify correct file copy to stage.
        Raises an exception in case of any mismatches"""

        folder_src = base_src / type_folder_name
        folder_dst = base_dst / type_folder_name

        src_files = get_all_files(folder_src)
        dst_files = get_all_files(folder_dst)

        src_checksums = get_all_checksums(path=folder_src, files=src_files)
        dst_checksums = get_all_checksums(path=folder_dst, files=dst_files)

        compare_checksums(
            src_checksums=src_checksums,
            dst_checksums=dst_checksums,
            folder_name_src=folder_name_src,
            folder_name_dst=folder_name_dst,
        )

    @task
    def del_files_from_ingest(base_src: ObjectStoragePath, type_folder_name: str):
        """Delete files from the ingestion location"""
        path = base_src / type_folder_name
        files = get_all_files(path)
        for f in files:
            f.unlink()

    files = list_ingest_files(base_path=BASE_SRC, image_folder=_IMAGE_FOLDER_NAME)
    copy_ingest_to_stage_obj = copy_ingest_to_stage.partial(base_dst=BASE_DST).expand(
        file=files
    )
    verify_checksum_obj = verify_checksum(
        base_src=BASE_SRC,
        base_dst=BASE_DST,
        type_folder_name=_IMAGE_FOLDER_NAME,
        folder_name_src=_INGEST_FOLDER_NAME,
        folder_name_dst=_STAGE_FOLDER_NAME,
    )
    del_files_from_ingest_obj = del_files_from_ingest(
        base_src=BASE_SRC, type_folder_name=_IMAGE_FOLDER_NAME
    )

    # ------------------------------ #
    # Define additional dependencies #
    # ------------------------------ #

    chain(copy_ingest_to_stage_obj, verify_checksum_obj, del_files_from_ingest_obj)


in_images()
