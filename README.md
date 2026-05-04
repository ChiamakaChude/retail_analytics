# Retail Data Platform (AWS Glue + Spark)

A production-style data engineering project that builds a scalable, metadata-driven ETL pipeline using Apache Spark on AWS Glue, with a layered data lake architecture (Bronze → Silver → Gold).

This project demonstrates real-world data engineering practices including:

Modular ETL design
Config-driven ingestion
Schema enforcement
Partitioned data lakes
Local testing with Spark
Cloud deployment with AWS


# Architecture Overview

Raw Data (S3 - Bronze)
        ↓
AWS Glue (Spark ETL Jobs)
        ↓
Processed Data (S3 - Silver, Parquet, Partitioned)
        ↓
Analytics Layer (Gold - Fact & Dimension Tables)

# Tech Stack

Python
Apache Spark (PySpark)
AWS Glue
Amazon S3
(Planned) dbt / Athena / Redshift


# Project Structure

retail-data-platform/
│
├── configs/                  # Configuration layer (WHAT to process)
│   ├── datasets.yaml           # Dataset metadata (paths, partitions, schemas)
│   └── schemas.yaml            # Spark schemas (StructType definitions)
│
├── etl/                      # Core ETL engine (HOW to process)
│   ├── engine.py             # Pipeline orchestration logic
│   ├── readers.py            # Data ingestion logic (S3 → Spark)
│   ├── writers.py            # Data output logic (Spark → S3)
│   ├── transforms.py         # Cleaning, joins, transformations
│   └── validators.py         # Data quality checks & validation
│
├── jobs/                     # Execution layer (Glue entrypoints)
│   └── glue_entry.py         # Main AWS Glue job script
│
├── local/                    # Local development & testing
│   └── run_local.py          # Run pipeline with Spark locally
│
├── tests/                    # Unit tests for ETL logic
│   └── test_orders.py
│
├── data/                     # Optional local sample data
│   └── orders.csv
│
├── utils/                     # Utils
│   ├── config.py              # Global variables store
│   ├── logging.py             # Logging configuration and structured logging function
│   ├── path_resolver.py       # Resolves data storage path (local/aws) depending on env
│   ├── schema_resolver.py     # Merges schema definitions yaml with dataset yaml
│   ├── spark_factory.py       # Chooses spark session based on env
│   ├── load_yaml.py           # Load yaml files with dataset configurations, schemas, paths
│   └── gen_env.py             # Get environment (local for testing or AWS gloe)
│
├── requirements.txt          # Python dependencies
├── README.md                 # Project documentation
└── etl_framework.zip         # Packaged ETL module (for Glue deployment)


# Design Principles

## 1. Separation of Concerns

configs/ → defines datasets and schemas
etl/ → reusable processing logic
jobs/ → execution layer (Glue-specific)

## 2. Metadata-Driven Pipelines

New datasets can be added via configuration without modifying core logic.

## 3. Reusable ETL Engine

All datasets are processed using a shared pipeline framework, avoiding duplication.

## 4. Local + Cloud Compatibility
Local testing with Spark (SparkSession)
Production execution in AWS Glue (GlueContext)


## 5. Data Lake Layering

### Layer	Description

Bronze	Raw ingested CSV files
Silver	Cleaned, deduplicated, partitioned Parquet
Gold	Analytics-ready fact and dimension tables


# How It Works

Define datasets in configs/datasets.py
Apply schemas from configs/schemas.py
Run ETL via:
    Local Spark (local/run_local.py)
    AWS Glue (jobs/glue_entry.py)
Data is written to S3 in partitioned Parquet format


# Deployment to AWS Glue

Package ETL modules:
zip -r etl_framework.zip etl/
Upload to Amazon S3
Attach ZIP in Glue job:
Python library path → s3://.../etl_framework.zip
Run Glue job using jobs/glue_entry.py


# Local Development

Install dependencies:

pip install -r requirements.txt

Run locally:

python local/run_local.py


# Future Enhancements

Add Gold layer modelling (fact + dimension tables)
Integrate dbt for transformation layer
Add Airflow orchestration
Implement data quality framework
Introduce CI/CD for Glue deployment
Add incremental processing (CDC / partition overwrite)


# Learning Outcomes

This project demonstrates:

Distributed data processing with Apache Spark
Cloud-native ETL pipelines using AWS Glue
Data lake architecture design
Modular pipeline engineering
Production-oriented data engineering practices


# Author

Chiamaka Amy Chude
Data Engineer

# Notes

This project is designed as a portfolio-ready data engineering system, reflecting real-world practices used in modern data platforms.