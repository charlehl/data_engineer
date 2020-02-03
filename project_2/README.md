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
table1 = """
    CREATE TABLE IF NOT EXISTS music_app_history 
    (artist_name varchar, song_title varchar, song_length float, sessionId int, itemInSession int, PRIMARY KEY(sessionId, itemInSession))
"""

table2 = """
    CREATE TABLE IF NOT EXISTS user_app_history 
    (artist_name varchar, song_title varchar, sessionId int, itemInSession int, first_name varchar, last_name varchar, userId int, PRIMARY KEY(userId, sessionId, itemInSession))
"""

table3 = """
    CREATE TABLE IF NOT EXISTS song_app_history 
    (artist_name varchar, song_title varchar, first_name varchar, last_name varchar, userId int, PRIMARY KEY(song_title, userId, artist_name))
"""
```

