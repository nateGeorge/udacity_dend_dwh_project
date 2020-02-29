-- create table statements as a .sql file for making ERD from DDL statements

CREATE TABLE IF NOT EXISTS staging_events
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

CREATE TABLE IF NOT EXISTS staging_songs
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

CREATE TABLE IF NOT EXISTS songplays
(songplay_id INT PRIMARY KEY,
start_time TIMESTAMP,
user_id INT NOT NULL,
level VARCHAR,
song_id VARCHAR,
artist_id VARCHAR,
session_id INT,
location VARCHAR,
user_agent VARCHAR);

CREATE TABLE IF NOT EXISTS users
(user_id INT PRIMARY KEY,
first_name VARCHAR,
last_name VARCHAR,
gender VARCHAR,
level VARCHAR);

CREATE TABLE IF NOT EXISTS songs
(song_id VARCHAR PRIMARY KEY,
title VARCHAR,
artist_id VARCHAR,
year INT,
duration NUMERIC);

CREATE TABLE IF NOT EXISTS artists
(artist_id VARCHAR PRIMARY KEY,
name VARCHAR,
location VARCHAR,
latitude NUMERIC,
longitude NUMERIC);


CREATE TABLE IF NOT EXISTS time
(time_id INT PRIMARY KEY,
start_time TIMESTAMP,
hour INT,
day INT,
week INT,
month INT,
year INT,
weekday INT);