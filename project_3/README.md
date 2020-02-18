# Project 3 README

## How to run project
- Run script create_tables.py
- Run scirpt etl.py
- Run queries in test_etl.ipynb to verify ETL

## File Overview
- etl.py - ETL script to read song and log data from S3 and load into a star schema DB in Amazon Redshift.
- create_tables.py - Contains SQL code for creating table, loading data into tables and also droping tables.
- test_etl.ipynb - Jupyter notebook containing queries to test data loaded from S3 to Amazon Redshift.
- dwh.cfg - Contains config for AWS and name for DB.  Will not be checked into repository for security purposes.
- project3_test.ipynb - Jupyter notebook to run experiments for ETL and develop code.
- README.md - Markdown README for project

## Schema Overview

### Database: dwh

### Tables:
```python
staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS events (
    songplay_id BIGINT IDENTITY(0,1),
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender VARCHAR,
    itemInSession INTEGER,
    lastName VARCHAR,
    length REAL,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration FLOAT8,
    sessionId INTEGER,
    song VARCHAR,
    status INTEGER,
    ts BIGINT,
    userAgent VARCHAR,
    userId INT,
    PRIMARY KEY(songplay_id)
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS song_logs (
    num_songs INTEGER,
    artist_id VARCHAR,
    artist_latitude REAL,
    artist_longitude REAL,
    artist_location VARCHAR,
    artist_name VARCHAR, 
    song_id VARCHAR,
    title VARCHAR,
    duration REAL,
    year INTEGER,
    PRIMARY KEY(song_id)
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays
(
    songplay_id BIGINT,
    start_time BIGINT NOT NULL,
    user_id INTEGER NOT NULL,
    level VARCHAR,
    song_id VARCHAR,
    artist_id VARCHAR,
    session_id INTEGER,
    location VARCHAR,
    user_agent VARCHAR,
    PRIMARY KEY(songplay_id),
    FOREIGN KEY(start_time) REFERENCES times(start_time),
    FOREIGN KEY(user_id) REFERENCES users(user_id),
    FOREIGN KEY(song_id) REFERENCES songs(song_id),
    FOREIGN KEY(artist_id) REFERENCES artists(artist_id)
)
DISTSTYLE KEY
DISTKEY(user_id)
COMPOUND SORTKEY(session_id, start_time);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER NOT NULL, 
    first_name VARCHAR, 
    last_name VARCHAR, 
    gender VARCHAR, 
    level VARCHAR,
    PRIMARY KEY(user_id)
)
DISTSTYLE AUTO
COMPOUND SORTKEY(last_name, first_name);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR, 
    title VARCHAR, 
    artist_id VARCHAR, 
    year INTEGER, 
    duration REAL,
    PRIMARY KEY(song_id)
)
DISTSTYLE AUTO
SORTKEY(year);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(
    artist_id VARCHAR, 
    name VARCHAR, 
    location VARCHAR, 
    lattitude REAL, 
    longitude REAL,
    PRIMARY KEY(artist_id)
)
DISTSTYLE AUTO
SORTKEY(name);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS times
(
    start_time BIGINT NOT NULL, 
    hour INTEGER, 
    day INTEGER, 
    week INTEGER, 
    month INTEGER, 
    year INTEGER, 
    weekday INTEGER,
    PRIMARY KEY(start_time)
)
DISTSTYLE KEY
DISTKEY(start_time)
SORTKEY(start_time);
""")
```

## Purpose of Project
To get practice with copying data from an S3 bucket to a staging table and then transfering the data to star-schema based database.
All is done in Python environment using boto3 to interact with AWS.  