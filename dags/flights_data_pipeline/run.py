from airflow.decorators import dag, task_group
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime
from pathlib import Path

from flights_data_pipeline.components.extract import Extract
from flights_data_pipeline.components.load import Load

# ========== Task Groups ==========

@task_group(group_id="extract_group")
def extract_group(table_list):
    for table in table_list:
        PythonOperator(
            task_id=f"extract_{table}",
            python_callable=Extract._pacflight_db,
            op_kwargs={'table_name': table}
        )

@task_group(group_id="load_group")
def load_group(table_list, table_pkey):
    from airflow.operators.python import PythonOperator
    load_tasks = []

    # task load for each table
    for table in table_list:
        task = PythonOperator(
            task_id=f"load_{table}",
            python_callable=Load._pacflight_db,
            op_kwargs={
                'table_name': table,
                'table_pkey': table_pkey
            }
        )
        load_tasks.append(task)

    # Set dependency sequential
    for i in range(1, len(load_tasks)):
        load_tasks[i - 1] >> load_tasks[i]

@task_group(group_id="transform_group")
def transform_group(transform_tables):
    for table in transform_tables:
        sql_file_path = f"/opt/airflow/dags/flights_data_pipeline/query/final/{table}.sql"
        try:
            sql_content = Path(sql_file_path).read_text()
        except FileNotFoundError:
            raise ValueError(f"SQL file not found for table: {table}")
        
        PostgresOperator(
            task_id=f"transform_{table}",
            postgres_conn_id='pacflight_db',
            sql=sql_content
        )

# ========== Main DAG ==========

@dag(
    dag_id='flights_data_pipeline',
    start_date=datetime(2025, 5, 15),
    schedule_interval='@daily',
    catchup=False,
    tags=['pacflight', 'ETL']
)
def flights_data_pipeline():
    # Define tables
    table_list = [
        'aircrafts_data',
        'airports_data',
        'bookings',
        'tickets',
        'seats',
        'flights',
        'ticket_flights',
        'boarding_passes'
    ]

    table_pkey = {
        "aircrafts_data": "aircraft_code",
        "airports_data": "airport_code",
        "bookings": "book_ref",
        "tickets": "ticket_no",
        "seats": ["aircraft_code", "seat_no"],
        "flights": "flight_id",
        "ticket_flights": ["ticket_no", "flight_id"],
        "boarding_passes": ["ticket_no", "flight_id"]
    }

    transform_tables = [
        'dim_aircrafts', 'dim_airport', 'dim_seat', 'dim_passenger',
        'fct_boarding_pass', 'fct_booking_ticket',
        'fct_seat_occupied_daily', 'fct_flight_activity'
    ]

    # Task groups
    extract = extract_group(table_list)
    load = load_group(table_list, table_pkey)
    transform = transform_group(transform_tables)

    # Task dependencies
    extract >> load >> transform

flights_data_pipeline()
