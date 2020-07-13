# Extract Transform & Load (ETL) for the Sparkify data logs

In the exercise revelvant information is extracted from data logs and below 5 tables are created in the Sparkify Database:

1. users 
2. songs
3. artists 
4. time
5. songplay

The data is modeled in Star Schema with songplay as the Fact Table with users, songs, artists as Dimension Table.
This helps us to get various analytical insights from the data with minimal joins.

## Steps for ETL

As per the instructions first step is to create the database structure in PostgreSQL by running in Terminal.

> python create_tables.py

Extract data from the log files.

> python etl.py

## Database Schema Design

### songplay Table

Table: songplay
Type: Fact Table

Column, Type, Description 

songplay_id, INTEGER, Identification of record in the table
start_time, TIMESTAMP, When the song was played
user_id, INTEGER, To identify the user
level, VARCHAR, Indicating the leve of membership
song_id, VARCHAR, Identifying the song played
artist_id, VARCHAR, Song by Artist
session_id, INTEGER, Identification for the session
location, VARCHAR, Location of the usage
user_agent, VARCHAR, Type of Devices used

### user Table

Table: user
Type: Dimension Table

Column, Type, Description 

user_id, integer PRIMARY KEY, Unique identifier for the user
first_name, VARCHAR, First Name of user
last_name, VARCHAR, Last name of user
gender, CHAR, FLAG value for sex of user(M/F)
level, VARCHAR, Indicating the leve of membership

### song Table

Table: song
Type: Dimension Table

Column, Type, Description 

song_id, VARCHAR NOT NULL PRIMARY KEY, Unique identifier for the song
title, VARCHAR, Name of the song
artist_id, VARCHAR, Song by Artist
year, integer, the year of song
duration, DECIMAL, duration of song in (seconds)


### artist Table

Table: artist
Type: Dimension Table

Column, Type, Description 

artist_id VARCHAR NOT NULL PRIMARY KEY, Song by Artist 
name VARCHAR, Name of Atrist
location VARCHAR, Location of Artist (City, State)
latitude DECIMAL, Geo loation (latitude)
longitude DECIMAL, Geo location (longitude)


### time Table

Table: time
Type: Dimension Table

Column, Type, Description 

start_time TIMESTAMP NOT NULL PRIMARY KEY, Timestamp value for start of activity 
hour numeric, Derived field Hour of start_time
day numeric, Derived field Day of start_time
week numeric, Derived field Week of start_time
month numeric, Derived field Month of start_time
year numeric, Derived field Year of start_time
weekday numeric, Derived field Weekday of start_time

## Queries leveraging Analytics

Q1. 
    a. Is the app more popular in certain regions, what is the frequency of hits lets say by State?

` %sql SELECT RIGHT(songplay.location, 2) AS state, \
        COUNT(songplay.session_id) AS freq \
        FROM songplay JOIN users ON users.user_id = songplay.user_id \
        GROUP BY 1 \
        ORDER BY 2 DESC;`

   b. Can we ge the users to this result as well?
   
` %sql SELECT \
        DISTINCT(users.user_id), \
        RIGHT(songplay.location, 2) AS state, \ 
        COUNT(songplay.session_id) AS freq \
        FROM songplay JOIN users ON users.user_id = songplay.user_id \
        GROUP BY 1,2 \
        ORDER BY 3 DESC;`
   
   c. How about top 2 users in each state ?
   
` %sql SELECT users.first_name, users.last_name, tmp.state, tmp.freq AS frequency, tmp.rw AS Rank_in_state \
              FROM users, \
               (SELECT \
                 DISTINCT(users.user_id), \
                 RIGHT(songplay.location, 2) AS state, \
                 COUNT(songplay.session_id) AS freq, \
                 ROW_NUMBER() OVER(PARTITION BY RIGHT(songplay.location, 2) ORDER BY COUNT(songplay.session_id) DESC) as rw \
                 FROM songplay JOIN users ON users.user_id = songplay.user_id \
                 GROUP BY 2,1 \
                 ORDER BY 3 DESC) tmp \
        WHERE users.user_id = tmp.u
        ser_id \
        AND tmp.rw <= 2 \
        ORDER BY tmp.state ASC, tmp.freq DESC;`
Q2. 
    a. Count of users using the app on Windows platform & free version?
   
 `%sql (SELECT COUNT(DISTINCT(users.user_id)) AS cnt\
        FROM songplay JOIN users ON users.user_id = songplay.user_id \
        WHERE songplay.user_agent LIKE ('%Win%')\
        AND users.level = 'free')`
   
   b. Count of users using the app on Safari browser & paid version?
   
  `%sql (SELECT COUNT(DISTINCT(users.user_id)) AS cnt\
        FROM songplay JOIN users ON users.user_id = songplay.user_id \
        WHERE songplay.user_agent LIKE ('%Safari%')\
        AND users.level = 'paid')`