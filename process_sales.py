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
    dag_id = "process_sales",
    default_args = default_args,
    description = 'final_taks'
)

load_csv_to_bq_bronze = GCSToBigQueryOperator(
    task_id = 'load_csv_to_bq_bronze',
    bucket = 'final_bucket-1',
    source_objects = ['sales_data/*.csv'],
    destination_project_dataset_table = 'de-07-dmytro-shpatakovskyi.bronze.sales',
    schema_fields = [
        {'name': 'CustomerId', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'PurchaseDate', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Product', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Price', 'type': 'STRING', 'mode': 'NULLABLE'},
    ],
    source_format = 'CSV',
    skip_leading_rows = 1,
    create_disposition = 'CREATE_IF_NEEDED',
    write_disposition = 'WRITE_TRUNCATE',
    dag=dag,
)

create_silver_sales_table = BigQueryCreateEmptyTableOperator(
    task_id="create_silver_sales_table",
    dataset_id="silver",
    table_id="sales",
    project_id="de-07-dmytro-shpatakovskyi",
    schema_fields=[
        {"name": "client_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "purchase_date", "type": "DATE", "mode": "NULLABLE"},
        {"name": "product_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "price", "type": "FLOAT64", "mode": "NULLABLE"},
    ],
    dag=dag,
)

transform_bronze_sales_data_to_silver = BigQueryOperator(
    task_id = 'transform_bronze_sales_data_to_silver',
    sql = '''
    INSERT INTO `de-07-dmytro-shpatakovskyi.silver.sales` (client_id, purchase_date, product_name, price)
    SELECT 
        CAST(CustomerId AS STRING) as client_id,
        SAFE.PARSE_DATE('%Y-%m-%d', TRIM(PurchaseDate)) as purchase_date,
        Product as product_name,
        CAST(REPLACE(REPLACE(Price, '$', ''), 'USD', '') AS FLOAT64) as price
    FROM `de-07-dmytro-shpatakovskyi.bronze.sales`
    ''',
    
    use_legacy_sql=False,
    dag=dag,
)

load_csv_to_bq_bronze >> create_silver_sales_table >> transform_bronze_sales_data_to_silver
