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


# Architecture Overview

Raw Data (S3 - Bronze)<br>
        ↓<br>
AWS Glue (Spark ETL Jobs)<br>
        ↓<br>
Processed Data (S3 - Silver, Parquet, Partitioned)<br>
        ↓<br>
Curated Data (S3 - Gold, Fact & Dimension Tables)<br>
        ↓<br>
Amazon Redshift (Analytics Warehouse)<br>

# Tech Stack

Python <br>
Apache Spark (PySpark) <br>
AWS Glue (Serverless Spark ETL) <br>
Amazon S3 (Data Lake Storage) <br>
Amazon Redshift (Data Warehouse) <br>
(YAML) Metadata Configuration Layer <br>
(Planned) dbt / Athena / Airflow <br>


# Project Structure

retail-data-platform/<br>
│<br>
├── configs/                    # Configuration layer (WHAT to process)<br>
│   ├── datasets.yaml           # Dataset metadata (paths, partitions, schemas)<br>
│   ├── models.yaml            # Redshift tables metadata (table names, load type etc)<br>
│   └── schemas.yaml            # Spark schemas (Data type definitions)<br>
│<br>
├── etl/                      # Core ETL engine (HOW to process)<br>
├──── raw_to_silver/<br> 
│       ├── engine.py             # Pipeline orchestration logic<br>
│       ├── readers.py            # Data ingestion logic (S3 → Spark)<br>
│       ├── writers.py            # Data output logic (Spark → S3)<br>
│       ├── transforms.py         # Cleaning, joins, transformations<br>
│       └── validators.py         # Data quality checks & validation<br>
├──── silver_to_gold/<br> 
│       ├── engine.py             # Pipeline orchestration logic<br>
│       ├── readers.py            # Data ingestion logic (S3 → Spark)<br>
│       ├── writers.py            # Data output logic (Spark → S3)<br>
│       ├── transforms.py         # Cleaning, joins, transformations<br>
│       └── validators.py         # Data quality checks & validation<br>
├──── gold_to_redshift/<br> 
│       ├── engine.py             # Pipeline orchestration logic<br>
│       ├── loaders.py            # Loading data into Redshift<br>
│       ├── readers.py            # Reading data from S3 <br>
│       └── validation.py         # Data quality checks & validation<br>
│<br>
├── jobs/                     # Execution layer (Glue entrypoints)<br>
│   ├── raw_to_silver.py      # Raw to silver layer job<br>
│   ├── silver_to_gold.py     # Silver to gold layer job<br>
│   └── gold_to_redshift.py   # Gold to redshift layer job<br>
│<br>
├── local/                    # Local development & testing<br>
│   └── run_local.py          # Run pipeline with Spark locally<br>
│<br>
├── tests/                    # Unit tests for ETL logic<br>
│   └── test_orders.py<br>
│<br>
├── data/                     # Optional local sample data<br>
│   └── orders.csv<br>
│<br>
├── utils/                     # Utils<br>
│   ├── config.py              # Global variables store<br>
│   ├── logging.py             # Logging configuration and structured logging function<br>
│   ├── path_resolver.py       # Resolves data storage path (local/aws) depending on env<br>
│   ├── schema_resolver.py     # Merges schema definitions yaml with dataset yaml<br>
│   ├── spark_factory.py       # Chooses spark session based on env<br>
│   ├── load_yaml.py           # Load yaml files with dataset configurations, schemas, paths<br>
│   ├── redshift_connection.py # Connect to Redshift logic<br>
│   └── get_env.py             # Get environment (local for testing or AWS Glue)<br>
│<br>
├── glue_entry.py             # Main AWS Glue job script<br>
├── local_entry.py            # # Run pipeline with Spark locally<br>
├── requirements.txt          # Python dependencies<br>
├── README.md                 # Project documentation<br>
└── etl_framework.zip         # Packaged ETL module (for Glue deployment)<br>


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


## 5. Data Lake Layering

### Layer	Description

Bronze	Raw ingested CSV files<br>
Silver	Cleaned, deduplicated, partitioned Parquet<br>
Gold	Analytics-ready fact and dimension tables


# How It Works

Define datasets in configs/datasets.py<br>
Apply schemas from configs/schemas.py<br>
Run ETL via:<br>
    Local Spark (local/run_local.py)<br>
    AWS Glue (jobs/glue_entry.py)<br>
Data is written to S3 in partitioned Parquet format


# Deployment to AWS Glue

Package ETL modules:<br>
zip -r etl_framework.zip etl/<br>
Upload to Amazon S3<br>
Attach ZIP in Glue job:<br>
Python library path → s3://.../etl_framework.zip<br>
Run Glue job using jobs/glue_entry.py<br>


# Local Development

Install dependencies:

pip install -r requirements.txt

Run locally:

python local/run_local.py


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
Data lake architecture design<br>
Modular pipeline engineering<br>
Production-oriented data engineering practices<br>


# Author

Chiamaka Amy Chude<br>
Data Engineer

# Notes

This project is designed as a portfolio-ready data engineering system, reflecting real-world practices used in modern data platforms.
