# nyc-taxi-data-pipeline
Hybrid batch + streaming pipeline for NYC TLC Yellow Taxi data. Batch: Cloud Composer + BigQuery for daily aggregates with data quality framework. Streaming: Pub/Sub + Cloud Functions for real-time ingestion. Data lake: Cloud Storage (bronze/silver/gold zones). Partitioning, clustering, CI/CD.
