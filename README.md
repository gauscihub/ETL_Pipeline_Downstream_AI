# ETL_Pipeline_Downstream_AI
Extracts data from web pages and makes available in parque files in S3 for downstream AI application

This project implements a scalable and serverless ETL pipeline on AWS to collect and pre-process Data Engineer related jobs posted on Adzuna using Adzuna API. The pipeline transforms raw JSON data into analytics-ready Parquet files, providing a reliable data foundation for downstream generative AI workflows.

# Architecture

Adzuna API → AWS Lambda → Amazon S3 (Raw) → AWS Glue (PySpark) → Amazon S3 (Parquet + Archive)
AWS Lambda: Extracts job data from Adzuna and stores raw JSON files in S3
Amazon S3: Stores raw, processed (Parquet), and archived data
AWS Glue (PySpark): Transforms raw JSON data into optimized Parquet datasets
AWS Lambda (Post-processing): Moves processed raw files to an archive location

# Workflow

Please refer to the workflow.svg uploaded in this repository for better understanding
