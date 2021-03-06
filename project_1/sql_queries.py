# Queries used for create_tables and etl scripts

# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
#timestamp, user ID, level, song ID, artist ID, session ID, location, and user agent
#ts, user_id, level, song_id, artist_id, session_id, location, user_agent
# Ideally song_id and artist_id would also be foreign keys but since we are using subset of data...we cannot
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id serial PRIMARY KEY, 
    ts bigint NOT NULL, 
    user_id int NOT NULL, 
    level varchar, 
    song_id varchar, 
    artist_id varchar, 
    session_id int, 
    location varchar, 
    user_agent varchar,
    FOREIGN KEY (ts) REFERENCES time (ts),
    FOREIGN KEY (user_id) REFERENCES users (user_id));
""")

#'userId', 'firstName', 'lastName', 'gender', 'level'
user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id int PRIMARY KEY, 
    first_name varchar, 
    last_name varchar, 
    gender varchar, 
    level varchar);
""")

#'song_id', 'title', 'artist_id', 'year', 'duration'
song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id varchar PRIMARY KEY, 
    title varchar, 
    artist_id varchar, 
    year int, 
    duration numeric)
""")

#'artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude'
artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id varchar PRIMARY KEY, 
    artist_name varchar, 
    artist_location varchar, 
    artist_latitude numeric, 
    artist_longitude numeric);
""")

#hour day week month year weekday
time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    ts bigint PRIMARY KEY, 
    hour int, 
    day int, 
    week int, 
    month int, 
    year int, 
    weekday int);
""")

# INSERT RECORDS
#ts, user_id, level, song_id, artist_id, session_id, location, user_agent
songplay_table_insert = ("""
INSERT INTO songplays (ts, user_id, level, song_id, artist_id, session_id, location, user_agent) 
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (songplay_id) DO NOTHING;
""")

#'userId', 'firstName', 'lastName', 'gender', 'level'
user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level) 
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (user_id) DO UPDATE SET level=EXCLUDED.level;
""")

#'song_id', 'title', 'artist_id', 'year', 'duration'
song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration) 
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (song_id) DO NOTHING;
""")

#'artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude'
artist_table_insert = ("""
INSERT INTO artists (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (artist_id) DO NOTHING;
""")

#hour day week month year weekday
time_table_insert = ("""
INSERT INTO time (ts, hour, day, week, month, year, weekday)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (ts) DO NOTHING;
""")

# FIND SONGS
#row.song, row.artist, row.length)
song_select = ("""
SELECT songs.song_id, songs.artist_id FROM songs
JOIN artists ON artists.artist_id = songs.artist_id
WHERE (songs.title = %s AND artists.artist_name = %s AND songs.duration = %s);
""")

# QUERY LISTS

create_table_queries = [user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]