
"""
Production DAG for NYC Taxi Data Pipeline
Batch processing: Bronze → Silver transformation
Schedule: Daily at 2 AM UTC
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.email import EmailOperator

# configuration
PROJECT_ID = "nyc-taxi-stream-pipeline-final"
DATASET = "nyc_data_final"
BRONZE_TABLE = f"{PROJECT_ID}.{DATASET}.nyc-taxi-data-final"
SILVER_TABLE = f"{PROJECT_ID}.{DATASET}.silver_nyc-taxi-data-final"
#SQL Script for Bronze to Silver Transformation
SILVER_SQL = f"""
CREATE OR REPLACE TABLE `{SILVER_TABLE}`
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID, payment_type
AS
SELECT 
    VendorID,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    RatecodeID,
    store_and_fwd_flag,
    PULocationID,
    DOLocationID,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    Airport_fee,
    CURRENT_TIMESTAMP() AS processed_timestamp,
    FORMAT_TIMESTAMP('%Y%%m%%d%%H%%M%%S', CURRENT_TIMESTAMP()) AS batch_id
FROM `{BRONZE_TABLE}`
WHERE 
    TIMESTAMP_DIFF(tpep_dropoff_datetime, tpep_pickup_datetime, second) > 0
    AND passenger_count > 0
    AND trip_distance > 0
    AND fare_amount > 0
"""

#DAG Definition

default_args = {
"owner":"data_engineering",
  "depends_on_past":False,
  "start_date":datetime(2026,4,13),
  "email_on_failure":True,
  "email_on_retry":False,
  "retries":2,
  "retry_delay":timedelta(minutes = 5),
  
}

dag = DAG(
  "nyc_taxi_batch_pipeline",
  default_args = default_args,
  description = "Production batch pipeline for NYC Taxi Data",
  schedule_interval = "0 2 * * *",
  catchup = False,
  max_active_runs = 1,
  tags = ["production","nyc_taxi","batch"]
)

# Tasks
start = DummyOperator(task_id = "start", dag = dag)
transform_to_silver = BigQueryInsertJobOperator(
  task_id = "transform_bronze_to_silver",
  configuration = {
    "query":
    {"query":SILVER_SQL,
     "useLegacySql":False
    }
  },
  dag = dag,
)
verify_quality = BigQueryInsertJobOperator(
    task_id="verify_data_quality",
    configuration={
        "query": {
            "query": f"""
                SELECT COUNT(*) AS fail_count
                FROM `{SILVER_TABLE}`
                WHERE passenger_count IS NULL OR passenger_count <= 0
                OR trip_distance IS NULL OR trip_distance <= 0
                OR fare_amount IS NULL OR fare_amount <= 0
            """,
            "useLegacySql": False
        }
    },
    dag=dag,
) 
send_alert = EmailOperator(
    task_id="send_success_email",
    to="thomasjosephgcp@gmail.com",
    subject="NYC Taxi Pipeline - Production Run Successful",
    html_content=f'<p>Silver table updated: {SILVER_TABLE}</p><p>Batch ID: {{{{ ds }}}}</p>',
    dag=dag,
)

end = DummyOperator(task_id="end", dag=dag)

# Dependencies
start >> transform_to_silver >> verify_quality >> send_alert >> end

"! Production ready v1.0" 
