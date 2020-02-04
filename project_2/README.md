# Project 2 README

## How to run project
- Run script etl.py
- Run scirpt etl_test.py

## File Overview
- etl.py - ETL script to read CSV in event_data folder and to load into an Apache Cassandra DB (sparkify_db)
- etl_test.py - Script to run test queries on data loaded by etl script.  Drops tables after running.
- event_datafile_new.csv - csv file generated using the project template
- my_event_datafile_new.csv - csv file generated using pandas instead of csv read/writer method in project template.
- Project_1B_Project_Template.ipynb - Notebook used to test etl methods
- README.md - Markdown README for project

## Schema Overview

### Database: sparkify_db

### Tables:
```python
# Description: Primary Key has two fields: sessionId is the partition key, and itemInSession is clustering key. Partitioning is done by sessionId and within that partition, rows are ordered by the itemInSession.
table1 = """
    CREATE TABLE IF NOT EXISTS music_app_history 
    (sessionId int, itemInSession int, artist_name varchar, song_title varchar, song_length float, PRIMARY KEY(sessionId, itemInSession))
"""

# Description: Primary Key has three fields: userId is the partition key, and sessionId and itemInSession are the clustering keys. Partitioning is done by userId and within that partition, rows are ordered by the sessionId and then by itemInSession.
table2 = """
    CREATE TABLE IF NOT EXISTS user_app_history 
    (userId int, sessionId int, itemInSession int, artist_name varchar, song_title varchar, first_name varchar, last_name varchar, PRIMARY KEY(userId, sessionId, itemInSession))
"""

# Description: Primary Key has two fields: song_title is the partition key, and userId is the clustering key. Partitioning is done by song_title and within that partition, rows are ordered by the userId.
table3 = """
    CREATE TABLE IF NOT EXISTS song_app_history 
    (song_title varchar, userId int, artist_name varchar, first_name varchar, last_name varchar, PRIMARY KEY(song_title, userId))
"""
```

## Purpose of Project
To get practice with creating denormalized databases on Apache Cassandra.  Source data is CSV which are loaded into Pandas, 
cleaned and then loaded into the approriate database.  Test queries are run on the data to verify that data was inserted properly 
and that partitioning key and clustering columns work as expected.
