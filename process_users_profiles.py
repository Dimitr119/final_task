from airflow import DAG
from airflow.contrib.operators.bigquery_operator  import (
    BigQueryCreateEmptyTableOperator,
    )
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.utils.dates import days_ago
from datetime import timedelta


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
    
}

dag = DAG(
    'process_user_profiles_pipeline',
    default_args=default_args,
    description='Load user profiles from JSON to Silver layer',
    schedule_interval=None,
    catchup=False,
    tags=['silver'],
)


create_silver_user_profiles_table = BigQueryCreateEmptyTableOperator(
    task_id="create_silver_user_profiles_table",
    dataset_id="silver",
    table_id="user_profiles",
    project_id="de-07-dmytro-shpatakovskyi",
    schema_fields=[
        {"name": "email", "type": "STRING", "mode": "NULLABLE"},
        {"name": "full_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "state", "type": "STRING", "mode": "NULLABLE"},
        {"name": "birth_date", "type": "DATE", "mode": "NULLABLE"},
        {"name": "phone_number", "type": "STRING", "mode": "NULLABLE"},
    ],
    bigquery_conn_id='google_cloud_default',
    dag=dag,
)

load_json_to_silver_user_profiles = GCSToBigQueryOperator(
    task_id='load_json_to_silver_user_profiles',
    bucket='final_bucket-1',
    source_objects=['users_data/*.json'],
    destination_project_dataset_table='de-07-dmytro-shpatakovskyi.silver.user_profiles',
    source_format='NEWLINE_DELIMITED_JSON',
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_TRUNCATE',    
    dag=dag,
)

enrich_customers_data = BigQueryOperator(
    task_id='enrich_customers_silver_data',
    sql="""
    MERGE INTO `de-07-dmytro-shpatakovskyi.silver.customers` AS C
    USING (
        SELECT
            P.email,
            SPLIT(P.full_name, ' ')[OFFSET(0)] AS first_name,
            SPLIT(P.full_name, ' ')[SAFE_OFFSET(1)] AS last_name,
            P.state,
            P.phone_number
        FROM `de-07-dmytro-shpatakovskyi.silver.user_profiles` AS P
    ) AS U
    ON C.email = U.email
    WHEN MATCHED THEN
        UPDATE SET
            C.first_name = COALESCE(C.first_name, U.first_name),
            C.last_name = COALESCE(C.last_name, U.last_name),
            C.state = COALESCE(C.state, U.state)
    """,
    use_legacy_sql=False,
    dag=dag,
)

create_silver_user_profiles_table >> load_json_to_silver_user_profiles >> enrich_customers_data

