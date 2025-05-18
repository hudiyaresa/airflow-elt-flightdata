from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from pendulum import datetime
from helper.minio import MinioClient
from helper.postgres import Execute

@dag(
    dag_id = 'pacflights_data_init',
    start_date = datetime(2025, 5, 15),
    schedule = "@once",
    catchup = False
)

def pacflights_data_init():
    # pacflight_create_funct = PythonOperator(
    #     task_id = 'dellstore_create_funct',
    #     python_callable = Execute._query,
    #     op_kwargs = {
    #         "connection_id": "pacflight_db",
    #         "query_path": "/profiling_quality_init/query/data_profile_quality_func.sql"
    #     }
    # )

    def create_bucket():
        minio_client = MinioClient._get()
        bucket_name = 'extracted-data'

        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
            
    # @task
    create_bucket_task = PythonOperator(
        task_id='create_bucket',
        python_callable=create_bucket,
    )

    create_pacflight_staging_table = PythonOperator(
        task_id='create_pacflight_staging_table',
        python_callable=Execute._query,
        op_kwargs={
            "connection_id": "warehouse_pacflight",
            "query_path": "/warehouse_init/query/stg_schema.sql"
        }
    )

    create_pacflight_warehouse_table = PythonOperator(
        task_id='create_pacflight_warehouse_table',
        python_callable=Execute._query,
        op_kwargs={
            "connection_id": "warehouse_pacflight",
            "query_path": "/warehouse_init/query/init.sql"
        }
    )

    create_bucket_task >> create_pacflight_staging_table >> create_pacflight_warehouse_table

pacflights_data_init()