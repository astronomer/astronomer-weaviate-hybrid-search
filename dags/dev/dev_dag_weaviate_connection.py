"""
## Dev DAG to test the Airflow - Weaviate connection
"""

from airflow.decorators import dag, task
from airflow.providers.weaviate.hooks.weaviate import WeaviateHook
from pendulum import datetime, duration
import os
import logging
import pandas as pd

t_log = logging.getLogger("airflow.task")

_WEAVIATE_CONN_ID = os.getenv("WEAVIATE_CONN_ID")


@dag(
    dag_display_name="🛠️ Dev DAG",
    start_date=datetime(2024, 7, 1),
    schedule="@daily",
    catchup=False,
    tags=["helper"],
)
def dev_dag_weaviate_connection():

    @task
    def test_weaviate_conn(conn_id: str) -> None:

        # connect to Weaviate using the Airflow connection `conn_id`
        hook = WeaviateHook(conn_id)

        # list all collections
        collections = hook.get_conn().collections.list_all()

        t_log.info(f"Collections: {collections}")

    test_weaviate_conn(
        conn_id=_WEAVIATE_CONN_ID,
    )


dev_dag_weaviate_connection()
