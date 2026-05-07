# Retail Data Platform (AWS Glue + Spark + Redshift)

A production-style data engineering platform that builds a scalable, metadata-driven ETL pipeline using Apache Spark on AWS Glue, implementing a layered data lake architecture (Bronze → Silver → Gold) with downstream loading into Amazon Redshift for analytics and warehousing.

This project demonstrates real-world data engineering practices including:

Modular ETL framework design <br>
Metadata-driven and config-based data ingestion <br>
Schema enforcement and validation across pipeline layers <br>
Partitioned data lake design for scalable processing <br>
Dimensional modelling (fact & dimension tables) in the Gold layer <br>
Automated data warehouse loading into Amazon Redshift (COPY + upsert patterns) <br>
Local development with Spark and production deployment on AWS <br>
Orchestration using AWS Glue Workflows<br>


# Architecture Overview

```text
Raw Data (S3 - Bronze)
        ↓
AWS Glue (Spark ETL Jobs)
        ↓
Processed Data (S3 - Silver, Parquet, Partitioned)
        ↓
Curated Data (S3 - Gold, Fact & Dimension Tables)
        ↓
Amazon Redshift (Analytics Warehouse)
        ↓
AWS Glue Workflow (Orchestration)
```

# Tech Stack

Python <br>
Apache Spark (PySpark) <br>
AWS Glue (Serverless Spark ETL) <br>
Amazon S3 (Data Lake Storage) <br>
Amazon Redshift (Data Warehouse) <br>
(YAML) Metadata Configuration Layer <br>
AWS Glue Workflows (Orchestration)<br>
(Planned) dbt / Athena / Airflow <br>


# Project Structure

```text
retail-data-platform/
│
├── configs/                    # Configuration layer (WHAT to process)
│   ├── datasets.yaml           # Dataset metadata (paths, partitions, schemas)
│   ├── models.yaml            # Redshift tables metadata (table names, load type etc)
│   └── schemas.yaml            # Spark schemas (Data type definitions)
│
├── etl/                      # Core ETL engine (HOW to process)
├──── raw_to_silver/ 
│       ├── engine.py             # Pipeline orchestration logic
│       ├── readers.py            # Data ingestion logic (S3 → Spark)
│       ├── writers.py            # Data output logic (Spark → S3)
│       ├── transforms.py         # Cleaning, joins, transformations
│       └── validators.py         # Data quality checks & validation
├──── silver_to_gold/ 
│       ├── engine.py             # Pipeline orchestration logic
│       ├── readers.py            # Data ingestion logic (S3 → Spark)
│       ├── writers.py            # Data output logic (Spark → S3)
│       ├── transforms.py         # Cleaning, joins, transformations
│       └── validators.py         # Data quality checks & validation
├──── gold_to_redshift/ 
│       ├── engine.py             # Pipeline orchestration logic
│       ├── loaders.py            # Loading data into Redshift
│       ├── readers.py            # Reading data from S3 
│       └── validation.py         # Data quality checks & validation
│
├── jobs/                     # Execution layer (Glue entrypoints)
│   ├── raw_to_silver.py      # Raw to silver layer job
│   ├── silver_to_gold.py     # Silver to gold layer job
│   └── gold_to_redshift.py   # Gold to redshift layer job
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
│   ├── redshift_connection.py # Connect to Redshift logic
│   └── get_env.py             # Get environment (local for testing or AWS Glue)
│
├── glue_entry.py             # Main AWS Glue job script
├── local_entry.py            # # Run pipeline with Spark locally
├── requirements.txt          # Python dependencies
├── README.md                 # Project documentation
└── etl_framework.zip         # Packaged ETL module (for Glue deployment)
```

# Design Principles

## 1. Separation of Concerns

configs/ → defines datasets and schemas<br>
etl/ → reusable processing logic<br>
jobs/ → execution layer (Glue-specific)<br>

## 2. Metadata-Driven Pipelines

New datasets can be added via configuration without modifying core logic.

## 3. Reusable ETL Engine

All datasets are processed using a shared pipeline framework, avoiding duplication.

## 4. Local + Cloud Compatibility
Local testing with Spark (SparkSession)<br>
Production execution in AWS Glue (GlueContext)


## 5. Data Lake + Warehouse Architecture

### Layer	Description

Bronze	        Raw ingested CSV files<br>
Silver	        Cleaned, deduplicated, partitioned Parquet<br>
Gold	        Analytics-ready fact and dimension tables<br>
Redshift        Columnar warehouse tables for analytics workloads

## 6. “Warehouse Loading Strategy”

Gold datasets are loaded into Amazon Redshift using:

COPY command for efficient bulk ingestion from S3 (Parquet format)<br>
Staging tables for intermediate loading<br>
Upsert (merge) logic to maintain consistency in fact and dimension tables<br>
Automated table creation based on schema definitions<br>

This ensures scalable and production-aligned data warehouse loading patterns.

## 7. Orchestrated Pipelines

Pipeline execution is coordinated using AWS Glue Workflows, enabling dependency management, failure handling, and scheduled runs across all ETL stages.

# How It Works

Define datasets and storage paths in configs/datasets.yaml<br>
Define schemas and warehouse models in configs/schemas.yaml and models.yaml<br>
Run ETL via:<br>
    Local Spark (run_local.py)<br>
    AWS Glue (glue_entry.py)<br>
Pipeline stages:<br>

Raw data is ingested into S3 (Bronze layer)<br>
Data is cleaned, validated, and written as partitioned Parquet (Silver layer)<br>
Business transformations produce fact and dimension tables (Gold layer)<br>
Gold datasets are loaded into Amazon Redshift using COPY and upsert patterns<br>
Pipeline execution is orchestrated via AWS Glue Workflows, ensuring sequential execution and failure handling across all stages.<br>

All processing is driven by metadata configuration, enabling reusable and extensible pipelines.


# Deployment to AWS Glue

Package ETL modules:<br>
zip -r etl_framework.zip etl/<br>
Upload to Amazon S3<br>
Attach ZIP in Glue job:<br>
Python library path → s3://.../etl_framework.zip<br>
Additional modules → psycopg2 (for Redshift connectivity)<br>
Run Glue job using glue_entry.py to dynamically execute pipeline stages.


# Local Development

Install dependencies:

pip install -r requirements.txt

Run locally:

python run_local.py


# Future Enhancements

Integrate dbt for transformation layer<br>
Add Airflow orchestration<br>
Implement data quality framework<br>
Introduce CI/CD for Glue deployment<br>
Add incremental processing (CDC / partition overwrite)<br>


# Learning Outcomes

This project demonstrates:

Distributed data processing with Apache Spark<br>
Cloud-native ETL pipelines using AWS Glue<br>
Design of scalable data lake and warehouse architectures<br>
Dimensional modelling for analytics workloads<br>
Metadata-driven pipeline design and extensibility<br>
Integration of data lake and data warehouse systems (S3 → Redshift)<br>
Production-oriented data engineering practices<br>


# Author

Chiamaka Amy Chude<br>
Data Engineer

# Notes

This project is designed as a portfolio-ready data engineering system, reflecting real-world practices used in modern data platforms.
