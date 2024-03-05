from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.contrib.operators.bigquery_operator import BigQueryCreateEmptyTableOperator

from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'enrich_customers_gold_pipeline',
    default_args=default_args,
    description='Create and enrich customers table in the Gold dataset',
    schedule_interval=None,
    catchup=False,
    tags=['gold'],
)

create_gold_customers_table = BigQueryCreateEmptyTableOperator(
    task_id="create_gold_customers_table",
    project_id="de-07-dmytro-shpatakovskyi",
    dataset_id="gold",
    table_id="customers",
    schema_fields=[
        {"name": "client_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "first_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "last_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "birth_date", "type": "DATE", "mode": "NULLABLE"},
        {"name": "email", "type": "STRING", "mode": "NULLABLE"},
        {"name": "registration_date", "type": "DATE", "mode": "NULLABLE"},
        {"name": "state", "type": "STRING", "mode": "NULLABLE"},
        {"name": "phone_number", "type": "STRING", "mode": "NULLABLE"},
    ],
    #bigquery_conn_id='google_cloud_default',
    dag=dag,
)

enrich_gold_customers_data = BigQueryInsertJobOperator(
    task_id='enrich_gold_customers_data',
    configuration={
        "query": {
            "query": """
                INSERT INTO `de-07-dmytro-shpatakovskyi.gold.customers`
                SELECT 
                    c.client_id,
                    c.first_name,
                    c.last_name,
                    p.birth_date,
                    c.email,
                    c.registration_date,
                    c.state,
                    p.phone_number
                FROM `de-07-dmytro-shpatakovskyi.silver.customers` c
                LEFT JOIN `de-07-dmytro-shpatakovskyi.silver.user_profiles` p ON c.email = p.email
            """,
            "useLegacySql": False,
        }
    },
    dag=dag,
)

create_gold_customers_table >> enrich_gold_customers_data
