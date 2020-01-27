# Queries used for create_tables and etl scripts

# DROP TABLES

songplay_table_drop = "DROP table songplays"
user_table_drop = "DROP table users"
song_table_drop = "DROP table songs"
artist_table_drop = "DROP table artists"
time_table_drop = "DROP table time"

# CREATE TABLES
#timestamp, user ID, level, song ID, artist ID, session ID, location, and user agent
#ts, user_id, level, song_id, artist_id, session_id, location, user_agent
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (ts bigint, user_id int, level varchar, song_id varchar, artist_id varchar, session_id int, location varchar, user_agent varchar);
""")

#'userId', 'firstName', 'lastName', 'gender', 'level'
user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (user_id int, first_name varchar, last_name varchar, gender varchar, level varchar);
""")

#'song_id', 'title', 'artist_id', 'year', 'duration'
song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (song_id varchar, title varchar, artist_id varchar, year int, duration numeric)
""")

#'artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude'
artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (artist_id varchar, artist_name varchar, artist_location varchar, artist_latitude numeric, artist_longitude numeric);
""")

#hour day week month year weekday
time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (ts bigint, hour int, day int, week int, month int, year int, weekday int);
""")

# INSERT RECORDS
#ts, user_id, level, song_id, artist_id, session_id, location, user_agent
songplay_table_insert = ("""
INSERT INTO songplays (ts, user_id, level, song_id, artist_id, session_id, location, user_agent) 
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
""")

#'userId', 'firstName', 'lastName', 'gender', 'level'
user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level) 
VALUES (%s, %s, %s, %s, %s)
""")

#'song_id', 'title', 'artist_id', 'year', 'duration'
song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration) 
VALUES (%s, %s, %s, %s, %s)
""")

#'artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude'
artist_table_insert = ("""
INSERT INTO artists (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
VALUES (%s, %s, %s, %s, %s)
""")

#hour day week month year weekday
time_table_insert = ("""
INSERT INTO time (ts, hour, day, week, month, year, weekday)
VALUES (%s, %s, %s, %s, %s, %s, %s)
""")

# FIND SONGS
#row.song, row.artist, row.length)
song_select = ("""
SELECT songs.artist_id, songs.song_id FROM songs
JOIN artists ON artists.artist_id = songs.artist_id
WHERE (songs.title = %s AND artists.artist_name = %s AND songs.duration = %s);
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]