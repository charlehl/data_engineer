# data_engineer
Repository for Data Engineering Projects and Learning

## Projects
- Project 1
  -ETL Script with Amazon Redshift: Created an ETL pipeline in Python for a database hosted on Redshift.  ETL consisted of loading data from S3 to staging tables on Redshift then executing SQL statements on staging tables to create analytics tables for a star-schema based design.  Created test queries to confirm ETL pipeline success.
- Project 2
  -ETL Script with Postgres: Created an ETL script to read music data from JSON sources and to store into a Postgres DB.  JSON files consisted of song data for music and log data for user events for music streaming service.  ETL script will locate all JSON source files for music and user events and parse each file.  The data will then be stored into a Postgres DB which is designed using a star schema optimized for song play analysis.
Project 3
  - ETL Script with Apache Cassandra: Created an ETL script to read data from CSV source files, clean and transform data to be stored in an Apache Cassandra backend.  Created tables to partition data based upon query requirements.  Also created a test script to query data from Cassandra backend for testing and validation purposes.
- Project 4
  -ETL Script Data Lake: Created an ETL script to load data from S3.  Data is cleaned and formatted to create tables for a star schema optimized queries for analysis and written to S3 for storage.  The ETL process is done using PySpark.
