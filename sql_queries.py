import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create = ("""CREATE TABLE IF NOT EXISTS staging_events
(artist VARCHAR,
auth VARCHAR,
firstName VARCHAR,
gender VARCHAR,
itemInSession INT,
lastName VARCHAR,
length NUMERIC,
level VARCHAR,
location VARCHAR,
method VARCHAR,
page VARCHAR,
registration NUMERIC,
sessionId INT,
song VARCHAR,
status INT,
ts BIGINT,
userAgent VARCHAR,
userId INT);
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs
(num_songs INT,
artist_id VARCHAR,
artist_latitude NUMERIC,
artist_longitude NUMERIC,
artist_location VARCHAR,
artist_name VARCHAR,
song_id VARCHAR,
title VARCHAR,
duration NUMERIC,
year INT);
""")

# wanted to make user_id NOT NULL but some of the values are null
songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays
(songplay_id INT IDENTITY(0, 1) PRIMARY KEY,
start_time TIMESTAMP,
user_id INT NOT NULL,
level VARCHAR,
song_id VARCHAR,
artist_id VARCHAR,
session_id INT,
location VARCHAR,
user_agent VARCHAR);
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users
(user_id INT PRIMARY KEY,
first_name VARCHAR,
last_name VARCHAR,
gender VARCHAR,
level VARCHAR);
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs
(song_id VARCHAR PRIMARY KEY,
title VARCHAR,
artist_id VARCHAR,
year INT,
duration NUMERIC);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists
(artist_id VARCHAR PRIMARY KEY,
name VARCHAR,
location VARCHAR,
latitude NUMERIC,
longitude NUMERIC);
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time
(time_id INT IDENTITY(0, 1) PRIMARY KEY,
start_time TIMESTAMP,
hour INT,
day INT,
week INT,
month INT,
year INT,
weekday INT);
""")

# STAGING TABLES
# for testing: 's3://udacity-dend/log_data/2018/11/2018-11-12-events.json'
# for copying all data: 's3://udacity-dend/log_data/'
staging_events_copy = ("""COPY staging_events FROM 's3://udacity-dend/log_data/'
credentials 'aws_iam_role={}'
json 's3://udacity-dend/log_json_path.json';
""").format(config.get('IAM_ROLE', 'ARN'))

staging_songs_copy = ("""COPY staging_songs FROM 's3://udacity-dend/song_data/'
credentials 'aws_iam_role={}'
json 'auto';
""").format(config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays
(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT TIMESTAMP 'epoch' + se.ts/1000 *INTERVAL '1 second', se.userId, se.level, ss.song_id, ss.artist_id, se.sessionId, se.location, se.userAgent
FROM staging_events se
JOIN staging_songs ss
ON (se.artist = ss.artist_name AND se.song = ss.title)
WHERE se.page = 'NextSong';
""")
# shouldn't need this anymore: AND se.userId IS NOT NULL;

user_table_insert = ("""
INSERT INTO users
(user_id, first_name, last_name, gender, level)
SELECT DISTINCT userId, firstName, lastName, gender, level
FROM staging_events
WHERE page = 'NextSong';
""")
# shouldn't need this anymore: AND userId IS NOT NULL

song_table_insert = ("""
INSERT INTO songs
(song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id, title, artist_id, year, duration
FROM staging_songs;
""")

artist_table_insert = ("""
INSERT INTO artists
(artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs;
""")

time_table_insert = ("""
INSERT INTO time
(start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT start_time as ts,
EXTRACT(hour from ts),
EXTRACT(day from ts),
EXTRACT(week from ts),
EXTRACT(month from ts),
EXTRACT(year from ts),
EXTRACT(weekday from ts)
FROM songplays;
""")
# shouldn't need this anymore: AND userId IS NOT NULL

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
