# Import Python packages 
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv
from cassandra.cluster import Cluster

def process_csv_data(session, dirpath):
    """
    Finds all CSV files located in relative directory path.
    Loads all CSV files into Pandas Dataframe, cleans data and uploads to approriate tables for project 2 assignment.
    
    Input:
    session - handle to Apache Cassandra cluster to insert data into tables
    dirpath - relative path directory
    
    """
    # Find all CSV
    filepath = os.getcwd() + dirpath
    print(f"Finding all CSV at path: {filepath}")
    file_path_list = []
    # Create a for loop to create a list of files and collect each filepath
    for root, dirs, files in os.walk(filepath):
        # Python notebook created temp save CSV so let's ignore those CSVs
        if(root.find("ipynb_checkpoints") == -1):
            for file in files:
                file_path_list.append(os.path.join(root, file))
    #print(file_path_list)
    # Read CSVs into Pandas and append
    col_names = ['artist', 'firstName', 'gender', 'itemInSession', 'lastName', 'length', \
           'level', 'location', 'sessionId', 'song', 'userId']
    # Create empty placeholder for csv
    full_df = pd.DataFrame(columns = col_names)
    for f in file_path_list:
        #print(f)
        df = pd.read_csv(f)
        # Drop rows where column artist is NaN, reset index for good measure and drop unneeded columns.
        df = df.dropna(subset=['artist']).reset_index(drop=True).drop(columns=['auth', 'method', 'page', 'registration', 'status', 'ts'])
        # Append to our final df
        full_df = full_df.append(df)

    # Convert to int32
    full_df = full_df.astype({'userId':'int32', 'sessionId': 'int32'})
    # round the length to 5 to copy the csv version
    full_df = full_df.round({'length': 5})
    print(f"{full_df.shape[0]} records found for upsert into tables...")
    
    print("Populate Tables with data...")
    # Create tables in new db and populate approriate data
    query1 = "INSERT INTO music_app_history (sessionId, itemInSession, artist_name, song_title, song_length) "
    query1 = query1 + "VALUES(%s, %s, %s, %s, %s)"
    
    query2 = "INSERT INTO user_app_history (userId, sessionId, itemInSession, artist_name, song_title, first_name, last_name) "
    query2 = query2 + "VALUES(%s, %s, %s, %s, %s, %s, %s)"

    query3 = "INSERT INTO song_app_history (song_title, userId, first_name, last_name) "
    query3 = query3 + "VALUES(%s, %s, %s, %s)"
    for row in full_df.itertuples(index=False):
        # Iterate through df once but load all tables in parallel
        session.execute(query1, (row[8], row[3], row[0], row[9], row[5]))
        session.execute(query2, (row[10], row[8], row[3], row[0], row[9], row[1], row[4]))
        session.execute(query3, (row[9], row[10], row[1], row[4]))
    print("All tables updated...")
    
def main():
    """ Main function for ETL script """
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
    
    # Create Tables for data
    query = """
        CREATE TABLE IF NOT EXISTS music_app_history 
        (sessionId int, itemInSession int, artist_name varchar, song_title varchar, song_length float, PRIMARY KEY(sessionId, itemInSession))
    """
    try:
        session.execute(query)
    except Exception as e:
        print(e)

    query2 = """
        CREATE TABLE IF NOT EXISTS user_app_history 
        (userId int, sessionId int, itemInSession int, artist_name varchar, song_title varchar, first_name varchar, last_name varchar, PRIMARY KEY(userId, sessionId, itemInSession))
    """
    try:
        session.execute(query2)
    except Exception as e:
        print(e)
 
    query3 = """
        CREATE TABLE IF NOT EXISTS song_app_history 
        (song_title varchar, userId int, artist_name varchar, first_name varchar, last_name varchar, PRIMARY KEY(song_title, userId))
    """
    try:
        session.execute(query3)
    except Exception as e:
        print(e)
    
    # Process CSV and load into Cassandra DB
    process_csv_data(session, '/event_data')
    
    session.shutdown()
    cluster.shutdown()


if __name__ == "__main__":
    main()