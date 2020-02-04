# Import Python packages 
import sys
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv
from cassandra.cluster import Cluster

def etl_run_test_queries(session):
    """ 
    Runs test queries for tables populated by etl script
    
    Input:
    session - session to Cassandra DB to run test queries on.
    """
    ## Test music_app_history table
    query_verify = "SELECT artist_name, song_title, song_length FROM music_app_history WHERE sessionId=338 AND itemInSession=4"
    try:
        rows = session.execute(query_verify)
        print(query_verify + "\n")
        for row in rows:
            print (f"{row.artist_name} | {row.song_title} | {row.song_length}")
        print("\n")
    except Exception as e:
        print(e)
    
    ## Test user_app_history table
    query_verify = "SELECT artist_name, song_title, first_name, last_name FROM user_app_history WHERE userId=10 AND sessionId=182"
    try:
        rows = session.execute(query_verify)
        print(query_verify + "\n")
        for row in rows:
            print (f"{row.artist_name} | {row.song_title} | {row.first_name} | {row.last_name}")
        print("\n")
    except Exception as e:
        print(e)
        
    query_verify = "SELECT first_name, last_name FROM song_app_history WHERE song_title = 'All Hands Against His Own'"
    try:
        rows = session.execute(query_verify)
        print(query_verify + "\n")
        for row in rows:
            print (f"{row.first_name} | {row.last_name}")
        print("\n")
    except Exception as e:
        print(e)

def drop_tables(session):
    """
    Drop all tables populated by ETL script
    
    Input:
    session - session to Cassandra DB to run queries on.
    """
    print("\n\nDroping all tables created by etl.py script...")
    
    query_drop = "DROP TABLE IF EXISTS music_app_history"
    try:
        rows = session.execute(query_drop)
    except Exception as e:
        print(e)

    query_drop = "DROP TABLE IF EXISTS user_app_history"
    try:
        rows = session.execute(query_drop)
    except Exception as e:
        print(e)

    query_drop = "DROP TABLE IF EXISTS song_app_history"
    try:
        rows = session.execute(query_drop)
    except Exception as e:
        print(e)

def main():
    """ Main function for ETL test script """
    cluster = Cluster(['127.0.0.1'])
    # To establish connection and begin executing queries, need a session
    session = cluster.connect()

    try:
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS sparkify_db 
            WITH REPLICATION = 
            { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
        )
    except Exception as e:
        print(e)
    
    # Connect to database
    try:
        session.set_keyspace('sparkify_db')
    except Exception as e:
        print(e)
    
    etl_run_test_queries(session)
    drop_tables(session)
    
    session.shutdown()
    cluster.shutdown()


if __name__ == "__main__":
    main()