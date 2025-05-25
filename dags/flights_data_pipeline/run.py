from airflow.decorators import dag, task_group
from airflow.operators.python import PythonOperator
from datetime import datetime

from flights_data_pipeline.components.extract import Extract
from flights_data_pipeline.components.load import Load
from flights_data_pipeline.components.transform import Transform

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
    load_tasks = []
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

    # Set sequential load due to FK dependencies
    for i in range(1, len(load_tasks)):
        load_tasks[i - 1] >> load_tasks[i]

@task_group(group_id="transform_group")
def transform_group(transform_tables):
    from airflow.operators.empty import EmptyOperator
    from airflow.models.baseoperator import chain

    previous = None
    for table in transform_tables:
        transform = Transform.build_operator(
            task_id=f"transform_{table}",
            table_name=table,
            sql_dir="flights_data_pipeline/query/final"
        )

        if previous:
            previous >> transform
        previous = transform


# ========== Main DAG ==========

@dag(
    dag_id='flights_data_pipeline',
    start_date=datetime(2025, 5, 15),
    schedule_interval='@daily',
    catchup=False,
    tags=['pacflight', 'ETL']
)
def flights_data_pipeline():
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
        'dim_aircraft', 'dim_airport', 'dim_seat', 'dim_passenger',
        'fct_boarding_pass', 'fct_booking_ticket',
        'fct_seat_occupied_daily', 'fct_flight_activity'
    ]

    # Run task groups
    extract = extract_group(table_list)
    load = load_group(table_list, table_pkey)
    transform = transform_group(transform_tables)

    # Dependency flow
    extract >> load >> transform

flights_data_pipeline()