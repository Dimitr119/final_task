from airflow.models.dag import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryCreateEmptyTableOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

dag = DAG(
    dag_id = "process_customers",
    default_args = default_args,
    description = 'final_task'
)

load_csv_to_bq_bronze_customers = GCSToBigQueryOperator(
    task_id='load_csv_to_bq_bronze_customers',
    bucket='final_bucket-1',
    source_objects=['customers_data/*.csv'],
    destination_project_dataset_table='de-07-dmytro-shpatakovskyi.bronze.customers',
    schema_fields=[
        {'name': 'Id', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'FirstName', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'LastName', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Email', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'RegistrationDate', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'State', 'type': 'STRING', 'mode': 'NULLABLE'},
    ],
    source_format='CSV',
    skip_leading_rows=1,
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_TRUNCATE',
    dag=dag,
)


create_silver_customers_table = BigQueryCreateEmptyTableOperator(
    task_id="create_silver_customers_table",
    dataset_id="silver",
    table_id="customers",
    project_id="de-07-dmytro-shpatakovskyi",
    schema_fields=[
        {"name": "client_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "first_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "last_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "email", "type": "STRING", "mode": "NULLABLE"},
        {"name": "registration_date", "type": "DATE", "mode": "NULLABLE"},
        {"name": "state", "type": "STRING", "mode": "NULLABLE"},
    ],
    dag=dag,
)

transform_bronze_customers_data_to_silver = BigQueryOperator(
    task_id='transform_bronze_customers_data_to_silver',
    sql='''
        MERGE `de-07-dmytro-shpatakovskyi.silver.customers` T
        USING (
            SELECT 
                CAST(Id AS STRING) as client_id,
                IFNULL(TRIM(FirstName), NULL) as first_name,
                IFNULL(TRIM(LastName), NULL) as last_name,
                IFNULL(TRIM(Email), NULL) as email,
                SAFE.PARSE_DATE('%Y-%m-%d', TRIM(RegistrationDate)) as registration_date,
                IFNULL(TRIM(State), NULL) as state
            FROM `de-07-dmytro-shpatakovskyi.bronze.customers`
        ) S
        ON T.client_id = S.client_id AND T.state = S.state
        WHEN MATCHED THEN
            UPDATE SET
                T.first_name = S.first_name,
                T.last_name = S.last_name,
                T.email = S.email,
                T.registration_date = S.registration_date
        WHEN NOT MATCHED THEN
            INSERT (client_id, first_name, last_name, email, registration_date, state)
            VALUES (S.client_id, S.first_name, S.last_name, S.email, S.registration_date, S.state);
    ''',
    use_legacy_sql=False,
    dag=dag,
)

load_csv_to_bq_bronze_customers >> create_silver_customers_table >> transform_bronze_customers_data_to_silver