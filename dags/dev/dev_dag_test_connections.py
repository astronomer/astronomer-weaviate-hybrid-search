"""
## Test connections

This DAG has tasks testing the Weaviate and Snowflake connections.
"""

from airflow.decorators import dag, task
from airflow.providers.weaviate.hooks.weaviate import WeaviateHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from pendulum import datetime, duration
import logging
import os

# Get the Airflow task logger
t_log = logging.getLogger("airflow.task")

# Weaviate variables
_WEAVIATE_CONN_ID = os.getenv("WEAVIATE_CONN_ID")

# Snowflake variables
_SNOWLFLAKE_CONN_ID = os.getenv("SNOWFLAKE_CONN_ID", "snowflake_default")
_SNOWFLAKE_DB_NAME = os.getenv("SNOWFLAKE_DB_NAME")
_SNOWFLAKE_SCHEMA_NAME = os.getenv("SNOWFLAKE_SCHEMA_NAME")

# -------------- #
# DAG definition #
# -------------- #


@dag(
    dag_display_name="🛠️ Test Connections",
    start_date=datetime(2024, 8, 1),
    schedule=None,
    catchup=False,
    default_args={"owner": "QA team", "retries": 3, "retry_delay": duration(minutes=1)},
    doc_md=__doc__,
    description="QA",
    tags=["qa", "helper"],
)
def dev_dag_test_connections():

    @task
    def test_weaviate_conn(conn_id: str) -> None:
        """
        Test Weaviate connection, log all collections.
        """

        # connect to Weaviate using the Airflow connection `conn_id`
        hook = WeaviateHook(conn_id)

        # list all collections
        collections = hook.get_conn().collections.list_all()

        t_log.info(f"Collections: {collections}")

    test_weaviate_conn(
        conn_id=_WEAVIATE_CONN_ID,
    )

    # test if the schema exists in Snowflake
    SQLExecuteQueryOperator(
        task_id="test_snowflake_conn",
        conn_id=_SNOWLFLAKE_CONN_ID,
        sql=f"""
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.SCHEMATA
            WHERE CATALOG_NAME = '{_SNOWFLAKE_DB_NAME}'
            AND SCHEMA_NAME = '{_SNOWFLAKE_SCHEMA_NAME}';

            """,
        show_return_value_in_logs=True,
        doc_md=f"Test if the schema {_SNOWFLAKE_DB_NAME}.{_SNOWFLAKE_SCHEMA_NAME} exists in Snowflake.",
    )


dev_dag_test_connections()
