from airflow.decorators import dag, task
from airflow.datasets import Dataset
from airflow.io.path import ObjectStoragePath
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.providers.weaviate.hooks.weaviate import WeaviateHook
from pendulum import datetime
import pandas as pd
import logging
import os

from include.functions.utils import read_and_encode_img

t_log = logging.getLogger("airflow.task")


_WEAVIATE_CONN_ID = os.getenv("WEAVIATE_CONN_ID")
_WEAVIATE_COLLECTION_NAME = "IMAGE_COLLECTION"
_CREATE_COLLECTION_TASK_ID = "create_collection"
_COLLECTION_ALREADY_EXISTS_TASK_ID = "collection_already_exists"
_AWS_CONN_ID = os.getenv("AWS_CONN_ID")
_S3_BUCKET = os.getenv("S3_BUCKET")
_STAGE_FOLDER_NAME = os.getenv("STAGE_FOLDER_NAME")
_TYPE_FOLDER_NAME = os.getenv("IMAGE_FOLDER_NAME")

OBJECT_STORAGE_SRC = "s3"
CONN_ID_SRC = _AWS_CONN_ID
KEY_SRC = _S3_BUCKET + "/" + _STAGE_FOLDER_NAME

BASE_SRC = ObjectStoragePath(
    f"{OBJECT_STORAGE_SRC}://{KEY_SRC}/{_TYPE_FOLDER_NAME}", conn_id=CONN_ID_SRC
)


@dag(
    dag_display_name="🏞️ Image Transform and Load to Weaviate",
    start_date=datetime(2024, 7, 1),
    schedule=[Dataset(BASE_SRC.as_uri())],
    catchup=False,
    tags=["transform", "load"],
)
def tl_image():

    # --------------- #
    # Set up Weaviate #
    # --------------- #

    @task.branch
    def check_collection(
        conn_id: str,
        collection_name: str,
        create_collection_task_id: str,
        collection_already_exists_task_id: str,
    ) -> str:
        """
        Check if the target collection exists in the Weaviate collection.
        Args:
            conn_id: The connection ID to use.
            collection_name: The name of the collection to check.
            create_collection_task_id: The task ID to execute if the collection does not exist.
            collection_already_exists_task_id: The task ID to execute if the collection already exists.
        Returns:
            str: Task ID of the next task to execute.
        """

        # connect to Weaviate using the Airflow connection `conn_id`
        hook = WeaviateHook(conn_id)

        # check if the collection exists in the Weaviate database
        collection = hook.get_conn().collections.exists(collection_name)

        if collection:
            t_log.info(f"Collection {collection_name} already exists.")
            return collection_already_exists_task_id
        else:
            t_log.info(f"Class {collection_name} does not exist yet.")
            return create_collection_task_id

    check_collection_obj = check_collection(
        conn_id=_WEAVIATE_CONN_ID,
        collection_name=_WEAVIATE_COLLECTION_NAME,
        create_collection_task_id=_CREATE_COLLECTION_TASK_ID,
        collection_already_exists_task_id=_COLLECTION_ALREADY_EXISTS_TASK_ID,
    )

    @task
    def create_collection(conn_id: str, collection_name: str) -> None:
        """
        Create a collection in the Weaviate schema.
        Args:
            conn_id: The connection ID to use.
            collection_name: The name of the collection to create.
            vectorizer: The vectorizer to use for the collection.
            schema_json_path: The path to the schema JSON file.
        """
        from weaviate.classes.config import Configure

        hook = WeaviateHook(conn_id)

        hook.create_collection(
            name=collection_name,
            vectorizer_config=Configure.Vectorizer.text2vec_openai(),
        )

    create_collection_obj = create_collection(
        conn_id=_WEAVIATE_CONN_ID,
        collection_name=_WEAVIATE_COLLECTION_NAME,
    )

    collection_already_exists = EmptyOperator(
        task_id=_COLLECTION_ALREADY_EXISTS_TASK_ID
    )

    weaviate_ready = EmptyOperator(task_id="weaviate_ready", trigger_rule="none_failed")

    chain(
        check_collection_obj,
        [create_collection_obj, collection_already_exists],
        weaviate_ready,
    )

    # ------------------ #
    # Ingest to Weaviate #
    # ------------------ #

    @task
    def list_folders(path: ObjectStoragePath) -> list[ObjectStoragePath]:
        """List files in local object storage."""
        folders = [f for f in path.iterdir() if f.is_dir()]
        return folders

    list_folders_obj = list_folders(path=BASE_SRC)

    @task(map_index_template="{{ my_custom_map_index }}")
    def transform_img(path: ObjectStoragePath) -> pd.DataFrame:
        """
        Extract text from markdown files in a folder.
        Args:
            folder_path (str): Path to the folder containing images.
        Returns:
            pd.DataFrame: A list of dictionaries containing the extracted information.
        """

        df = read_and_encode_img(path=path, content_type="image", encoding="utf-8")

        # get the current context and define the custom map index variable
        from airflow.operators.python import get_current_context

        context = get_current_context()
        context["my_custom_map_index"] = f"Extracted files from: {path}."

        return df

    transform_img_obj = transform_img.expand(path=list_folders_obj)

    @task
    def ingest_img(df: pd.DataFrame, conn_id: str, collection_name: str):
        hook = WeaviateHook(conn_id)

        collection = hook.get_conn().collections.get(collection_name)

        for index, row in df.iterrows():

            collection.data.insert(
                properties={
                    "folder_path": row["folder_path"],
                    "title": row["title"],
                    "image": row["image"],
                },
            )

    ingest_img_obj = ingest_img.partial(
        conn_id=_WEAVIATE_CONN_ID, collection_name=_WEAVIATE_COLLECTION_NAME
    ).expand(df=transform_img_obj)

    @task(
        outlets=[
            Dataset(
                f"weaviate://{_WEAVIATE_CONN_ID}@{_WEAVIATE_COLLECTION_NAME}/{_TYPE_FOLDER_NAME}"
            )
        ]
    )
    def ingest_done(ingest_type: str):
        t_log.info(f"Ingestion of {ingest_type} done!")

    chain(
        [transform_img_obj, weaviate_ready],
        ingest_img_obj,
        ingest_done(ingest_type=_TYPE_FOLDER_NAME),
    )


tl_image()
