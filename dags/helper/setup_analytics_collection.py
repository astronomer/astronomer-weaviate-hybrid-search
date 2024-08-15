"""
### Demo Setup DAG: Upload search history

This DAG loads a demo search history into a Weaviate collection.
"""

from airflow.decorators import dag, task
from airflow.datasets import Dataset
from airflow.io.path import ObjectStoragePath
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.providers.weaviate.hooks.weaviate import WeaviateHook
from airflow.providers.weaviate.operators.weaviate import WeaviateIngestOperator
from pendulum import datetime, duration
import pandas as pd
import logging
import os

# import modularized functions from the include folder
from include.functions.utils import read_files_from_path

# Get the Airflow task logger
t_log = logging.getLogger("airflow.task")

# Weaviate variables
_WEAVIATE_CONN_ID = os.getenv("WEAVIATE_CONN_ID")
_WEAVIATE_ANALYTICS_COLLECTION_NAME = os.getenv("WEAVIATE_ANALYTICS_COLLECTION_NAME")

# S3 variables
_AWS_CONN_ID = os.getenv("AWS_CONN_ID")
_S3_BUCKET = os.getenv("S3_BUCKET")
_MOCK_SEARCH_HISTORY_FOLDER = os.getenv("MOCK_SEARCH_HISTORY_FOLDER")
_SEARCH_HISTORY_FOLDER = os.getenv("SEARCH_HISTORY_FOLDER")

# Creating ObjectStoragePath objects
# See https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/objectstorage.html
# for more information on the Airflow Object Storage feature
OBJECT_STORAGE_SRC = "s3"
CONN_ID_SRC = _AWS_CONN_ID
KEY_SRC = _S3_BUCKET + "/" + _MOCK_SEARCH_HISTORY_FOLDER + "/" + _SEARCH_HISTORY_FOLDER

BASE_SRC = ObjectStoragePath(f"{OBJECT_STORAGE_SRC}://{KEY_SRC}/", conn_id=CONN_ID_SRC)

# branching task IDs
_CREATE_COLLECTION_TASK_ID = "create_collection"
_COLLECTION_ALREADY_EXISTS_TASK_ID = "collection_already_exists"

# -------------- #
# DAG definition #
# -------------- #


@dag(
    dag_display_name="🛠️ Set up Analytics collection",
    start_date=datetime(2024, 8, 1),
    schedule=[
        Dataset(
            f"s3://{_S3_BUCKET}/{_MOCK_SEARCH_HISTORY_FOLDER}/{_SEARCH_HISTORY_FOLDER}"
        )
    ],
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
def setup_analytics_collection():

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
        collection_name=_WEAVIATE_ANALYTICS_COLLECTION_NAME,
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
        from weaviate.classes.config import Property, DataType
        import weaviate.classes.config as wvcc

        hook = WeaviateHook(conn_id)

        hook.create_collection(
            name=_WEAVIATE_ANALYTICS_COLLECTION_NAME,
            vectorizer_config=wvcc.Configure.Vectorizer.text2vec_openai(model="ada"),
            generative_config=wvcc.Configure.Generative.openai(
                model="gpt-4-1106-preview"
            ),
            properties=[
                Property(name="searchterm", data_type=DataType.TEXT),
                Property(name="timestamp", data_type=DataType.TEXT),
                Property(
                    name="purchase", data_type=DataType.BOOL, skip_vectorization=True
                ),
                Property(
                    name="amt", data_type=DataType.NUMBER, skip_vectorization=True
                ),
                Property(name="uuid", data_type=DataType.UUID, skip_vectorization=True),
            ],
        )

    create_collection_obj = create_collection(
        conn_id=_WEAVIATE_CONN_ID,
        collection_name=_WEAVIATE_ANALYTICS_COLLECTION_NAME,
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
    def list_files(path: ObjectStoragePath) -> list[ObjectStoragePath]:
        """List files in local object storage."""
        folders = [f for f in path.iterdir() if f.is_file()]
        return folders

    list_files_obj = list_files(path=BASE_SRC)

    @task(map_index_template="{{ my_custom_map_index }}")
    def extract_document_text(path: ObjectStoragePath) -> pd.DataFrame:
        """
        Extract text from markdown files in a folder.
        Args:
            folder_path (str): Path to the folder containing markdown files.
        Returns:
            pd.DataFrame: A list of dictionaries containing the extracted information.
        """

        df = read_files_from_path(path=path, content_type="text", encoding="utf-8")

        t_log.info(f"Number of records: {df.shape[0]}")
        t_log.info(f"Head: {df.head()}")

        # get the current context and define the custom map index variable
        from airflow.operators.python import get_current_context

        context = get_current_context()
        context["my_custom_map_index"] = f"Extracted files from: {path}."

        return df

    extract_document_text_obj = extract_document_text.expand(path=list_files_obj)

    ingest_data = WeaviateIngestOperator.partial(
        task_id="ingest_data",
        conn_id=_WEAVIATE_CONN_ID,
        collection_name=_WEAVIATE_ANALYTICS_COLLECTION_NAME,
    ).expand(input_data=extract_document_text_obj)

    @task(
        outlets=[
            Dataset(
                f"weaviate://{_WEAVIATE_CONN_ID}@{_WEAVIATE_ANALYTICS_COLLECTION_NAME}/"
            )
        ]
    )
    def ingest_done():
        t_log.info(f"Ingestion done!")


    # ------------------------------ #
    # Define additional dependencies #
    # ------------------------------ #

    chain(
        weaviate_ready,
        ingest_data,
        ingest_done(),
    )


setup_analytics_collection()
