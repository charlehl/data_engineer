import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS events CASCADE"
staging_songs_table_drop = "DROP TABLE IF EXISTS song_logs CASCADE"
songplay_table_drop = "DROP TABLE IF EXISTS songplays CASCADE"
user_table_drop = "DROP TABLE IF EXISTS users CASCADE"
song_table_drop = "DROP TABLE IF EXISTS songs CASCADE"
artist_table_drop = "DROP TABLE IF EXISTS artists CASCADE"
time_table_drop = "DROP TABLE IF EXISTS times CASCADE"

# CREATE TABLES

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

# STAGING TABLES

staging_events_copy = ("""
COPY events FROM {}
CREDENTIALS {}
JSON {}
""").format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
COPY song_logs FROM {}
CREDENTIALS {}
JSON 'auto'
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

# TBD
songplay_table_insert = ("""
INSERT INTO songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT songplay_id, ts, userId, level, song_id, artist_id, sessionId, location, userAgent
FROM events
FULL JOIN song_logs ON events.artist = song_logs.artist_name AND events.song = song_logs.title
WHERE userId IS NOT NULL;
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT userId, firstName, lastName, gender, level
FROM events WHERE userId IS NOT NULL 
ORDER BY sessionId DESC;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id, title, artist_id, year, duration
FROM song_logs;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, lattitude, longitude)
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM song_logs;
""")

time_table_insert = ("""
INSERT INTO times (start_time, hour, day, week, month, year, weekday)
SELECT ts, extract(hour from ts_timestamp), extract(day from ts_timestamp), extract(week from ts_timestamp), extract(month from ts_timestamp), extract(year from ts_timestamp), extract(weekday from ts_timestamp)
FROM (SELECT DISTINCT ts, TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS ts_timestamp FROM events);
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
