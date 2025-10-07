from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.dummy_operator import DummyOperator

# Default arguments for DAG tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Project and dataset definitions
project_id = 'sales-analytics-473222'
dataset_id = 'staging_dataset'
transform_dataset_id = 'transform_dataset'
reporting_dataset_id = 'processing_dataset'
source_table = f'{project_id}.{dataset_id}.global_data'

# If you want to dynamically fetch countries, you can get the distinct countries
# from the source_table after it is loaded, otherwise hardcode a list
countries = ['USA', 'India', 'Germany', 'Japan', 'France', 'Canada', 'Italy']

# DAG definition
with DAG(
    dag_id='automatic_data_load_and_transformation',
    default_args=default_args,
    description='Load CSV from GCS to BigQuery, create country-specific tables and views dynamically',
    schedule_interval='0 0 * * *',  # Daily at midnight
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['bigquery', 'gcs', 'csv'],
) as dag:

    # Task 1: Check if the CSV file exists in GCS
    check_file_exists = GCSObjectExistenceSensor(
        task_id='check_file_exists',
        bucket='bucket-for-global-data',
        object='global_health_data.csv',
        timeout=300,
        poke_interval=30,
        mode='poke',
    )

    # Task 2: Load CSV to staging table
    load_csv_to_bigquery = GCSToBigQueryOperator(
        task_id='load_csv_to_bq',
        bucket='bucket-for-global-data',
        source_objects=['global_health_data.csv'],
        destination_project_dataset_table=source_table,
        source_format='CSV',
        allow_jagged_rows=True,
        ignore_unknown_values=True,
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        field_delimiter=',',
        autodetect=True,
    )

    # Lists to store dynamically generated tasks
    create_table_tasks = []
    create_view_tasks = []

    # Task 3: Dynamically create country-specific tables and views
    for country in countries:
        # Create table for each country
        create_table_task = BigQueryInsertJobOperator(
            task_id=f'Tabular_information_{country.lower()}',
            configuration={
                "query": {
                    "query": f"""
                        CREATE OR REPLACE TABLE `{project_id}.{transform_dataset_id}.{country.lower()}_table` AS
                        SELECT *
                        FROM `{source_table}`
                        WHERE country = '{country}'
                    """,
                    "useLegacySql": False,
                }
            },
        )

        # Create view for reporting per country
        create_view_task = BigQueryInsertJobOperator(
            task_id=f'create_view_{country.lower()}',
            configuration={
                "query": {
                    "query": f"""
                        CREATE OR REPLACE VIEW `{project_id}.{reporting_dataset_id}.{country.lower()}_view` AS
                        SELECT
                            `Year` AS `year`,
                            `Disease Name` AS `disease_name`,
                            `Disease Category` AS `disease_category`,
                            `Prevalence Rate` AS `prevalence_rate`,
                            `Incidence Rate` AS `incidence_rate`
                        FROM `{project_id}.{transform_dataset_id}.{country.lower()}_table`
                        WHERE `Availability of Vaccines Treatment` = False
                    """,
                    "useLegacySql": False,
                }
            },
        )

        # Set dependencies: load CSV → create table → create view
        create_table_task.set_upstream(load_csv_to_bigquery)
        create_view_task.set_upstream(create_table_task)

        # Append to lists for setting final success task
        create_table_tasks.append(create_table_task)
        create_view_tasks.append(create_view_task)

    # Dummy task to indicate DAG success
    success_task = DummyOperator(task_id='success_task')

    # Define overall flow
    check_file_exists >> load_csv_to_bigquery
    for table_task, view_task in zip(create_table_tasks, create_view_tasks):
        table_task >> view_task >> success_task
