# Project 1 README

## How to run project
- Run script create_tables.py
- Run script etl.py

## File Overview
- create_tables.py - Script to drop database and all tables if they exist in order to provide a clean start for project.
- etl.ipynb - Notebook used to develop etl methods
- etl.py - ETL script for moving song data from JSON format to Postgres SQL DB
- sql_queries.py - Queries used for both scripts
- test.ipynb - Notebook used to test etl methods


## Schema Overview

### Database: sparkifydb

### Tables:

#### Fact Tables
**songplays**
- songplay_id serial (PK)
- ts - timestamp of event
- user_id - unique user id
- level - type of membership for user
- song_id - unique song id
- artist_id - unique artist id
- session_id - unique session id
- location - location of user
- user_agent - method of interface to program

#### Dimension Tables
**user**
- user_id - unique user id (PK)
- first_name - first name of user
- last_name - last_name of user
- gender - gender of user
- level - type of membership for user

**song**
- song_id - unique song id (PK)
- title - title of song
- artist_id - unique artist id
- year - year of song release
- duration - duration of song (in seconds?)

**artist**
- artist_id - unique artist id (PK)
- artist_name - name of artist
- artist_location - location of artist
- artist_latitude - lat of artist location
- artist_longitude - long of artist location

**time**
- ts - timestamp of event (PK)
- hour - hour of ts
- day - day of ts
- week -  week of ts
- month - month of ts
- year -  year of ts
- weekday - weekday of ts

## Purpose of the database
Purpose of the database is to create a database optimized for queries on song play analysis.  This was done using a star schema.

## Justify database schema design and ETL pipeline
The songplays table allows the user to query and analyze song play usage by several different groupings.  Whether grouping is by membership type, location of users or usage length, 
all of these are available by a simple query of the songplay fact table.  If more detailed analysis is requested, then a simple aggregation of the dimension tables will make it easy to 
aggregate any sort of specific data necessary.

## Improvements
Since the data source was only partial for the songs, the majority of the records for the songplay table do not specify artist_id, song_id.  This prevented me from specifying the 
aforementioned fields as foreign keys.  It would be nice to have a complete song library and to be able to specify the foreign key relationships properly.