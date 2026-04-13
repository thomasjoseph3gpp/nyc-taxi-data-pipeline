# nyc-taxi-data-pipeline
Hybrid batch + streaming pipeline for NYC TLC Yellow Taxi data. Batch: Cloud Composer + BigQuery for daily aggregates with data quality framework. Streaming: Pub/Sub + Cloud Functions for real-time ingestion. Data lake: Cloud Storage (bronze/silver/gold zones). Partitioning, clustering, CI/CD.


# NYC Taxi Data Pipeline

[![Python Version](https://img.shields.io/badge/python-3.11-blue.svg)](https://python.org)
[![Airflow Version](https://img.shields.io/badge/airflow-2.10.5-blue.svg)](https://airflow.apache.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-green.svg)](LICENSE)

## Overview

Production-grade hybrid data pipeline for NYC TLC Yellow Taxi trip data. Processes 2.9M+ monthly records through both batch and streaming paths.

**Key Features:**
- ✅ Batch processing (daily at 2 AM UTC)
- ✅ Streaming ingestion (real-time via Pub/Sub)
- ✅ Data quality framework (bronze → silver → gold)
- ✅ Partitioning and clustering for query optimization
- ✅ CI/CD with Cloud Build
- ✅ Infrastructure as Code (Terraform)

## Architecture

┌─────────────────────────────────────────────────────────────┐
│ NYC TLC Taxi Data │
└─────────────────────┬───────────────────┬───────────────────┘
│ │
▼ ▼
┌─────────────┐ ┌─────────────┐
│ Batch │ │ Streaming │
│ (Daily) │ │ (Real-time) │
└──────┬──────┘ └──────┬──────┘
│ │
▼ ▼
┌─────────────┐ ┌─────────────┐
│Cloud Storage│ │ Pub/Sub │
│ (Raw Zone) │ │ Topic │
└──────┬──────┘ └──────┬──────┘
│ │
▼ ▼
┌─────────────┐ ┌─────────────┐
│ BigQuery │ │ BigQuery │
│ Bronze │◄──────│ Streaming │
└──────┬──────┘ │ Buffer │
│ └─────────────┘
▼
┌─────────────┐
│ BigQuery │
│ Silver │
└──────┬──────┘
│
▼
┌─────────────┐
│ BigQuery │
│ Gold │
└─────────────┘

text

## Technology Stack

| Layer | Technology |
|-------|------------|
| **Orchestration** | Cloud Composer (Apache Airflow 2.10.5) |
| **Storage** | Cloud Storage (Data Lake) |
| **Warehouse** | BigQuery (Partitioned + Clustered) |
| **Streaming** | Pub/Sub + Cloud Functions |
| **CI/CD** | Cloud Build + GitHub |
| **Infrastructure** | Terraform |
| **Language** | Python 3.11 |

## Repository Structure
nyc-taxi-data-pipeline/
├── batch/
│ ├── dags/ # Airflow DAG definitions
│ ├── sql/ # SQL queries for transformations
│ └── scripts/ # Batch download scripts
├── streaming/
│ ├── pubsub/ # Pub/Sub publisher scripts
│ ├── cloud_functions/ # Cloud Function code
│ └── sql/ # Streaming sink definitions
├── shared/
│ ├── config/ # Configuration files
│ ├── tests/ # Unit and integration tests
│ └── requirements.txt # Python dependencies
├── infrastructure/
│ └── terraform/ # Infrastructure as Code
├── cloudbuild.yaml # CI/CD pipeline definition
├── README.md
└── LICENSE

text

## Data Quality Framework

| Zone | Description |
|------|-------------|
| **Bronze** | Raw data as ingested, no transformations |
| **Silver** | Cleaned, validated, DQ flags added |
| **Gold** | Aggregated business metrics |

**DQ Rules Applied:**
- `passenger_count > 0`
- `trip_distance > 0.0`
- `fare_amount > 0`
- `trip_duration > 0 seconds`

## Getting Started

### Prerequisites

- Google Cloud Platform account (free tier eligible)
- GitHub account
- Local terminal with `gcloud` SDK installed

### Environment Setup

```bash
# Clone repository
git clone https://github.com/YOUR_USERNAME/nyc-taxi-data-pipeline.git
cd nyc-taxi-data-pipeline

# Set up Cloud Composer environment
gcloud composer environments create nyc-taxi-pipeline-env \
    --location=us-central1 \
    --image-version=composer-3-airflow-2.10.5 \
    --environment-size=small

# Deploy DAGs
gsutil -m rsync -r batch/dags/ gs://[DAGS_BUCKET]/dags/
Pipeline Execution
Batch Pipeline (Daily at 2 AM UTC)
python
# DAG: nyc_taxi_batch_pipeline
# Tasks:
1. wait_for_file          # Check for new parquet file
2. load_bronze_table      # Load raw data to BigQuery
3. transform_to_silver    # Clean and validate
4. verify_data_quality    # Run DQ checks
5. send_notification      # Email on success/failure
Streaming Pipeline (Real-time)
bash
# Publish simulated ride events
python streaming/pubsub/publish_rides.py --rate 10
Monitoring
Airflow UI: https://composer.googleusercontent.com (from environment details)

BigQuery Console: Query performance and partition analysis

Cloud Logging: Pipeline execution logs

Cloud Monitoring: Custom metrics for DQ pass/fail rates

CI/CD Pipeline
Cloud Build automatically:

Validates DAG syntax on every push

Runs unit tests

Deploys to Cloud Composer on merge to main

Updates Airflow variables

Performance Optimization
Feature	Implementation
Partitioning	DATE(tpep_pickup_datetime)
Clustering	VendorID, payment_type
Table Design	Bronze (raw) → Silver (cleaned) → Gold (aggregated)
Contributing
Create feature branch: git checkout -b feature/description

Commit changes: git commit -m 'feat: add something'

Push branch: git push origin feature/description

Open Pull Request

License
Apache License 2.0

Author
[Thomas Joseph]

Acknowledgments
NYC TLC for open taxi trip data

Google Cloud for educational credits


