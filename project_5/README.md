# Project 5 README

# Overview

![image](example-dag.png)

## How to run project
- Start up Airflow
- In Air

## File Overview
- dags directory
    - create_tables.sql - sql statements used to create all tables for Redshift
    - sparkify_etl_dag - DAG for Sparkify ETL.
- plugins directory
    - helpers directory
        - sql_queries.py - Contains sql queries used to create fact and dimension tables from staging tables.
    - opeartors directory
        - data_quality.py - operator used to run data quality checks
        - load_dimension.py - operator used to load dimension tables
        - load_fact.py - operator used to load fact tables
        - stage_redshift.py - operator used to copy data from S3 to Redshift staging tables
- README.md - Markdown README for project