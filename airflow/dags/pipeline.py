from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from scripts.ingest import ingest_mysql, ingest_mongo
from scripts.etl import etl
from scripts.load_to_bigquery import create_bigquery_tables


args = {
    "owner": "osmani",
    "start_date": days_ago(1),
    "max_active_runs": 1,
    "concurrency": 1,
}
dag = DAG(
    dag_id="pipeline_dag",
    default_args=args,
    schedule_interval="*/10 * * * *",  # * * * * * *
)

with dag:
    mysql_ingestion = PythonOperator(
        task_id="mysql_ingestion", python_callable=ingest_mysql, dag=dag
    )

    mongo_ingestion = PythonOperator(
        task_id="mongo_ingestion", python_callable=ingest_mongo, dag=dag
    )

    extract_transform_load = PythonOperator(
        task_id="extract_transform_load", python_callable=etl, dag=dag
    )

    load_to_bigquery = PythonOperator(
        task_id="load_to_bigquery", python_callable=create_bigquery_tables, dag=dag
    )

[mysql_ingestion, mongo_ingestion] >> extract_transform_load >> load_to_bigquery
