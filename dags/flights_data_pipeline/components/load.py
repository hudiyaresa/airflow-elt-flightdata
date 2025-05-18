from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException, AirflowException
from helper.minio import CustomMinio
import logging
import json
import ast
import pandas as pd

class Execute:
    @staticmethod
    def _insert_dataframe(connection_id, query_path, dataframe):
        BASE_PATH = "/opt/airflow/dags"
        pg_hook = PostgresHook(postgres_conn_id=connection_id)
        connection = pg_hook.get_conn()
        cursor = connection.cursor()

        full_path = f'{BASE_PATH}/{query_path}'
        with open(full_path, 'r') as file:
            query = file.read()

        try:
            for _, row in dataframe.iterrows():
                record = row.to_dict()

                # Convert dictionary-style strings to JSON-compatible format
                for key, value in record.items():
                    if isinstance(value, str):
                        try:
                            # Attempt to load and then dump again to ensure valid JSON format
                            record[key] = json.dumps(json.loads(value)) if value.startswith('{') else value
                        except json.JSONDecodeError:
                            # If the value isn't valid JSON, just leave it as it is
                            pass

                # Run the query with the sanitized data
                pg_hook.run(query, parameters=record)

        except Exception as e:
            logging.error(f"Error executing query: {e}")
            raise AirflowException(f"Error when loading data: {str(e)}")

        finally:
            cursor.close()
            connection.commit()
            connection.close()


import json
import ast

class Load:
    @staticmethod
    def _pacflight_db(table_name, **kwargs):
        logging.info(f"[Load] Starting full load for table: {table_name}")

        try:
            object_name = f'/temp/{table_name}.csv'
            bucket_name = 'extracted-data'

            # Load CSV from MinIO to DataFrame
            logging.info(f"[Load] Downloading {object_name} from {bucket_name}")
            df = CustomMinio._get_dataframe(bucket_name, object_name)

            if df.empty:
                raise AirflowSkipException(f"{table_name} has no data to load. Skipped...")

            # Perbaiki data JSON pada kolom 'model' menggunakan ast.literal_eval()
            if table_name == 'aircrafts_data':
                df['model'] = df['model'].apply(lambda x: json.dumps(ast.literal_eval(x)) if isinstance(x, str) else x)

            if table_name == 'airports_data':
                df['airport_name'] = df['airport_name'].apply(lambda x: json.dumps(ast.literal_eval(x)) if isinstance(x, str) else x)
                df['city'] = df['city'].apply(lambda x: json.dumps(ast.literal_eval(x)) if isinstance(x, str) else x)

            if table_name == 'tickets':
                df['contact_data'] = df['contact_data'].apply(lambda x: json.dumps(ast.literal_eval(x)) if isinstance(x, str) else x)

            if table_name == 'flights':
                df = df.replace({float('nan'): None})

            # Path to query insert
            query_path = f"flights_data_pipeline/query/stg/{table_name}.sql"

            # Execute insert query
            Execute._insert_dataframe(
                connection_id="warehouse_pacflight",
                query_path=query_path,
                dataframe=df
            )

            logging.info(f"[Load] Full load completed for table: {table_name}")

        except AirflowSkipException as e:
            logging.warning(f"[Load] Skipped loading for {table_name}: {str(e)}")
            raise e

        except Exception as e:
            logging.error(f"[Load] Failed loading {table_name}: {str(e)}")
            raise AirflowException(f"Error when loading {table_name} : {str(e)}")
