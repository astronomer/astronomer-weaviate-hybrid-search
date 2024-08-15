"""
### ETL: Archive raw product information

This DAG
- Verifies all product information files from the S3 ingest location have been
correctly copied to the S3 stage location
- Deletes files from the S3 ingest location
"""

from airflow.decorators import dag, task
from airflow.datasets import Dataset
from airflow.io.path import ObjectStoragePath
from airflow.models.baseoperator import chain
from pendulum import datetime, duration
import logging
import os

# import modularized functions from the include folder
from include.functions.utils import get_all_files, get_all_checksums, compare_checksums

# Get the Airflow task logger
t_log = logging.getLogger("airflow.task")

# Weaviate variables
_WEAVIATE_CONN_ID = os.getenv("WEAVIATE_CONN_ID")
_WEAVIATE_COLLECTION_NAME_PRODUCT_INFO = os.getenv(
    "WEAVIATE_COLLECTION_NAME_PRODUCT_INFO"
)

# S3 variables
_AWS_CONN_ID = os.getenv("AWS_CONN_ID")
_S3_BUCKET = os.getenv("S3_BUCKET")
_STAGE_FOLDER_NAME = os.getenv("STAGE_FOLDER_NAME")
_ARCHIVE_FOLDER_NAME = os.getenv("ARCHIVE_FOLDER_NAME")
_PRODUCT_INFO_FOLDER_NAME = os.getenv("PRODUCT_INFO_FOLDER_NAME")

# Creating ObjectStoragePath objects
# See https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/objectstorage.html
# for more information on the Airflow Object Storage feature
OBJECT_STORAGE_SRC = "s3"
CONN_ID_SRC = _AWS_CONN_ID
KEY_SRC = _S3_BUCKET + "/" + _STAGE_FOLDER_NAME

OBJECT_STORAGE_DST = "s3"
CONN_ID_DST = _AWS_CONN_ID
KEY_DST = _S3_BUCKET + "/" + _ARCHIVE_FOLDER_NAME


BASE_SRC = ObjectStoragePath(f"{OBJECT_STORAGE_SRC}://{KEY_SRC}", conn_id=CONN_ID_SRC)
BASE_DST = ObjectStoragePath(f"{OBJECT_STORAGE_DST}://{KEY_DST}", conn_id=CONN_ID_DST)

# -------------- #
# DAG definition #
# -------------- #


@dag(
    dag_display_name="🗄️ Archive raw product information",
    start_date=datetime(2024, 8, 1),
    schedule=[
        Dataset(
            f"weaviate://{_WEAVIATE_CONN_ID}@{_WEAVIATE_COLLECTION_NAME_PRODUCT_INFO}/"
        )
    ],
    catchup=False,
    default_args={"owner": "DE team", "retries": 3, "retry_delay": duration(minutes=1)},
    doc_md=__doc__,
    description="ETL",
    tags=["etl", "use-case"],
)
def archive_stage_text():

    @task
    def list_ingest_folders(
        base_path: ObjectStoragePath, image_folder: str
    ) -> list[ObjectStoragePath]:
        """List files in remote object storage."""
        path = base_path / image_folder
        folders = [f for f in path.iterdir() if f.is_dir()]
        return folders

    @task
    def copy_ingest_to_stage(
        path_src: ObjectStoragePath, base_dst: ObjectStoragePath
    ) -> None:
        """Copy a file from remote to local storage.
        The file is streamed in chunks using shutil.copyobj"""

        for f in path_src.iterdir():
            full_key = base_dst / os.path.join(*f.parts[-3:])
            f.copy(dst=full_key)

    @task(outlets=Dataset(BASE_DST.as_uri() + "/" + _PRODUCT_INFO_FOLDER_NAME))
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
        path = base_src / type_folder_name
        files = get_all_files(path)
        for f in files:
            f.unlink()

    folders = list_ingest_folders(
        base_path=BASE_SRC, image_folder=_PRODUCT_INFO_FOLDER_NAME
    )
    copy_ingest_to_stage_obj = copy_ingest_to_stage.partial(base_dst=BASE_DST).expand(
        path_src=folders
    )
    verify_checksum_obj = verify_checksum(
        base_src=BASE_SRC,
        base_dst=BASE_DST,
        type_folder_name=_PRODUCT_INFO_FOLDER_NAME,
        folder_name_src=_STAGE_FOLDER_NAME,
        folder_name_dst=_ARCHIVE_FOLDER_NAME,
    )
    del_files_from_ingest_obj = del_files_from_ingest(
        base_src=BASE_SRC, type_folder_name=_PRODUCT_INFO_FOLDER_NAME
    )

    # ------------------------------ #
    # Define additional dependencies #
    # ------------------------------ #

    chain(copy_ingest_to_stage_obj, verify_checksum_obj, del_files_from_ingest_obj)


archive_stage_text()
