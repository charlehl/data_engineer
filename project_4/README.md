# Project 4 README

## How to run project
- Start Up EMR
- Login to EMR cluster
- Change to use Python3 for Spark
- Copy Over etl.py and dl.cfg to cluster
- spark-submit etl.py

## File Overview
- etl.py - ETL script to read song and log data from S3 and load into a star schema DB in Amazon Redshift.
- README.md - Markdown README for project
- dl.cfg - Not checked into repository, contains AWS access key and secret access key.

```
# File Format for dl.cfg:
[AWS]
AWS_ACCESS_KEY_ID=<KEY>
AWS_SECRET_ACCESS_KEY=<SECRET_KEY>
```

## ETL Table Schema

### Fact Table
- songplays - records in log data associated with song plays
    - Columns: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables
- users - users in the app
    - Columns: user_id, first_name, last_name, gender, level
- songs - songs in music database
    - Columns: song_id, title, artist_id, year, duration
- artists - artists in music database
    - Columns: artist_id, name, location, lattitude, longitude
- time - timestamps of records in songplays broken down into specific units
    - Columns: start_time, hour, day, week, month, year, weekday


# Purpose of Project
To get practice with copying data from an S3 bucket to then transforming data and then lastly, transfering the data to star-schema based tables stored in parquet format in an S3 bucket.
All is done in Python environment using pyspark to interact with AWS.
