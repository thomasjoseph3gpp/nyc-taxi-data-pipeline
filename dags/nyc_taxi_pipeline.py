
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
GOLD_DAILY_TABLE = f"{PROJECT_ID}.{DATASET}.gold_daily_metrics"
GOLD_LOCATION_TABLE = f"{PROJECT_ID}.{DATASET}.gold_location_metrics"
GOLD_HOURLY_TABLE = f"{PROJECT_ID}.{DATASET}.gold_hourly_metrics"
#SQL Script for Bronze to Silver Transformation
SILVER_SQL = f"""
CREATE OR REPLACE TABLE `{SILVER_TABLE}`
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID, payment_type, dq_status
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
    FORMAT_TIMESTAMP('%Y%%m%%d%%H%%M%%S', CURRENT_TIMESTAMP()) AS batch_id,
    "PASS" AS dq_status
FROM `{BRONZE_TABLE}`
WHERE 
    TIMESTAMP_DIFF(tpep_dropoff_datetime, tpep_pickup_datetime, second) > 0
    AND passenger_count > 0
    AND trip_distance > 0
    AND fare_amount > 0
"""

GOLD_DAILY_SQL = f"""

CREATE OR REPLACE TABLE `nyc-taxi-stream-pipeline-final.nyc_data_final.gold_daily_metrics` 

CLUSTER BY VendorID 
AS
SELECT 
DATE(tpep_pickup_datetime) AS trip_date,
VendorID,
COUNT(*) AS total_trips,
SUM(passenger_count) AS total_passengers,
ROUND(AVG(passenger_count),2) AS avg_passengers,
ROUND(SUM(trip_distance),2) AS total_distance,
ROUND(AVG(trip_distance),2) AS avg_distance,
ROUND(SUM(fare_amount),2) AS total_fare,
ROUND(AVG(fare_amount),2) AS avg_fare,
ROUND(SUM(tip_amount),2) AS total_tip,
ROUND(AVG(tip_amount),2) AS avg_tip,
ROUND(SAFE_DIVIDE(SUM(tip_amount),SUM(fare_amount))*100,2) AS tip_percentage,
COUNT(DISTINCT PULocationID) AS unique_pickup_locations,
COUNT(DISTINCT DOLocationID ) AS unique_dropoff_locations
FROM `nyc-taxi-stream-pipeline-final.nyc_data_final.silver_nyc-taxi-data-final`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2024-01-01' AND '2024-01-31'
GROUP BY DATE(tpep_pickup_datetime), VendorID
"""

GOLD_LOCATION_SQL = f"""
CREATE OR REPLACE TABLE `nyc-taxi-stream-pipeline-final.nyc_data_final.gold_location_metrics`
CLUSTER BY location_id
AS  
WITH locations_stats AS (

SELECT
PULocationID as location_id,
'pickup' AS location_type,
COUNT(*) AS total_trips,
ROUND(SUM(fare_amount),2) AS total_fare,
ROUND(AVG(fare_amount),2) AS avg_fare,
ROUND(SUM(trip_distance),2) AS total_distance,
ROUND(AVG(trip_distance),2) AS avg_distance,
ROUND(SUM(tip_amount),2) AS total_tip,
ROUND(SAFE_DIVIDE(SUM(tip_amount), SUM(fare_amount))*100,2) AS avg_tip_percentage
FROM `nyc-taxi-stream-pipeline-final.nyc_data_final.silver_nyc-taxi-data-final`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2024-01-01' AND '2024-01-31'
GROUP BY PULocationID


UNION ALL

SELECT
DOLocationID as location_id,
'dropoff' AS location_type,
COUNT(*) AS total_trips,
ROUND(SUM(fare_amount),2) AS total_fare,
ROUND(AVG(fare_amount),2) AS avg_fare,
ROUND(SUM(trip_distance),2) AS total_distance,
ROUND(AVG(trip_distance),2) AS avg_distance,
ROUND(SUM(tip_amount),2) AS total_tip,
ROUND(SAFE_DIVIDE(SUM(tip_amount), SUM(fare_amount))*100,2) AS avg_tip_percentage
FROM `nyc-taxi-stream-pipeline-final.nyc_data_final.silver_nyc-taxi-data-final`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2024-01-01' AND '2024-01-31'
GROUP BY DOLocationID
)
SELECT *, ROW_NUMBER() OVER( ORDER BY total_trips DESC) AS rank_by_trips
FROM locations_stats
"""

GOLD_HOURLY_SQL = f"""

CREATE OR REPLACE TABLE `nyc-taxi-stream-pipeline-final.nyc_data_final.gold_hourly_metrics`
CLUSTER BY trip_hour, is_weekend

AS
SELECT 
EXTRACT(HOUR FROM tpep_pickup_datetime) AS trip_hour,
EXTRACT(DAYOFWEEK FROM tpep_pickup_datetime) AS day_of_week,
CASE WHEN EXTRACT(DAYOFWEEK FROM tpep_pickup_datetime)  IN (1,7) THEN TRUE ELSE FALSE END AS is_weekend,
COUNT(*) AS total_trips,
ROUND(AVG(TIMESTAMP_DIFF(tpep_dropoff_datetime,tpep_pickup_datetime,MINUTE)),2) AS avg_trip_duration_minutes,
ROUND(AVG(trip_distance),2) AS avg_distance,
ROUND(AVG(fare_amount),2) AS avg_fare,
ROUND(SAFE_DIVIDE(SUM(tip_amount), SUM(fare_amount)) * 100, 2) AS avg_tip_percentage
FROM `nyc-taxi-stream-pipeline-final.nyc_data_final.silver_nyc-taxi-data-final`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2024-01-01' AND '2024-01-31'
GROUP BY trip_hour, day_of_week, is_weekend
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

transform_to_gold_daily  = BigQueryInsertJobOperator(
    task_id = "transform_to_gold_daily",
    configuration = {
        "query":{
            "query":GOLD_DAILY_SQL,
            "useLegacySql": False
        }
    },
    dag = dag,
)
transform_to_gold_location = BigQueryInsertJobOperator(
    task_id = "transform_to_gold_location",
    configuration:{
        "query":{
            "query":GOLD_LOCATION_SQL,
            "useLegacySql":False
        }
    },
    dag = dag,
)
transform_to_gold_hourly = BigQueryInsertJobOperator(
    task_id = "transform_to_gold_hourly",
    configuration = {
        "query":{
        "query":GOLD_HOURLY_SQL,
        "useLegacySql":False
    }
    },
    dag = dag,
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
start >> transform_to_silver >> verify_quality 

verify_quality >> [transform_to_gold_daily, transform_to_gold_location, transform_to_gold_hourly] >> send_alert

send_alert >> end

"! Production ready v1.0" 
"# Force trigger build" 
