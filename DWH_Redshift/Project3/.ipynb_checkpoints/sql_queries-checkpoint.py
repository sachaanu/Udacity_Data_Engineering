import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

S3_LOG_DATA=config.get('S3', 'LOG_DATA')
S3_LOG_JSONPATH=config.get('S3', 'LOG_JSONPATH')
S3_SONG_DATA=config.get('S3', 'SONG_DATA')
DWH_IAM_ROLE_ARN = config.get('IAM_ROLE', 'ARN')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events(
                            artist  VARCHAR,
                            auth    VARCHAR,
                            firstName VARCHAR,
                            gender CHAR,
                            iteminSession INTEGER,
                            lastName VARCHAR,
                            length FLOAT,
                            level VARCHAR,
                            location VARCHAR,
                            method VARCHAR,
                            page VARCHAR,
                            registration BIGINT,
                            sessionid INTEGER,
                            song VARCHAR,
                            status INTEGER,
                            ts TIMESTAMP,
                            userAgent VARCHAR,
                            userid INTEGER);
""")
  
staging_songs_table_create = ("""
CREATE TABLE staging_songs
(
song_id VARCHAR,
num_songs INTEGER,
                            title VARCHAR,
                            artist_name VARCHAR,
                            artist_latitude FLOAT,
                            year INTEGER,
                            duration FLOAT,
                            artist_id VARCHAR,
                            artist_longitude FLOAT,
                            artist_location VARCHAR);
""")

    
user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (user_id INTEGER NOT NULL,
                                    first_name VARCHAR,
                                    last_name VARCHAR,
                                    gender CHAR,
                                    level VARCHAR,
                                    PRIMARY KEY (user_id)
                                    );
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (song_id VARCHAR NOT NULL,
                                    title VARCHAR NOT NULL,
                                    artist_id VARCHAR,
                                    year INTEGER,
                                    duration FLOAT,
                                    PRIMARY KEY (song_id))
                                    SORTKEY (title);
""")


artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (artist_id VARCHAR,
                                    name VARCHAR NOT NULL,
                                    location VARCHAR,
                                    latitude FLOAT,
                                    longitude FLOAT,
                                    PRIMARY KEY (artist_id))
                                    SORTKEY (name);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (start_time TIMESTAMP NOT NULL,
                                    hour NUMERIC NOT NULL,
                                    day NUMERIC NOT NULL,
                                    week NUMERIC NOT NULL,
                                    month NUMERIC NOT NULL,
                                    year NUMERIC NOT NULL,
                                    weekday NUMERIC NOT NULL,
                                    PRIMARY KEY (start_time))
                                    SORTKEY (start_time);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay (songplay_id INTEGER IDENTITY(0,1),
                                    start_time TIMESTAMP,
                                    user_id INTEGER,
                                    level VARCHAR,
                                    song_id VARCHAR,
                                    artist_id VARCHAR,
                                    session_id INTEGER,
                                    location VARCHAR,
                                    user_agent VARCHAR,
                                    PRIMARY KEY (songplay_id),
                                    FOREIGN KEY (song_id) REFERENCES songs (song_id),
                                    FOREIGN KEY (artist_id) REFERENCES artists (artist_id))
                                    SORTKEY (songplay_id);
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from {}
    region 'us-west-2'
    iam_role '{}'
    compupdate off statupdate off truncatecolumns blanksasnull emptyasnull
    format as json {}
    timeformat as 'epochmillisecs'
""").format(S3_LOG_DATA, DWH_IAM_ROLE_ARN, S3_LOG_JSONPATH)

staging_songs_copy = ("""
copy staging_songs from {}
    region 'us-west-2'
    iam_role '{}'
    compupdate off statupdate off truncatecolumns blanksasnull emptyasnull
    format as json 'auto'
""").format(S3_SONG_DATA, DWH_IAM_ROLE_ARN)

S3_LOG_DATA=config.get('S3', 'LOG_DATA')
S3_LOG_JSONPATH=config.get('S3', 'LOG_JSONPATH')
S3_SONG_DATA=config.get('S3', 'SONG_DATA')
DWH_IAM_ROLE_ARN = config.get('IAM_ROLE', 'ARN')


# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT
    to_timestamp(to_char(e.ts, '9999-99-99 99:99:99'),'YYYY-MM-DD HH24:MI:SS') AS start_time,
    e.userId as user_id,
    e.level,
    s.song_id as song_id,
    s.artist_id as artist_id,
    e.sessionId as sesion_id,
    e.location as location,
    e.userAgent as user_agent
FROM 
    staging_events e, staging_songs s
WHERE
    e.page = 'NextSong'
AND e.song = s.title;
""")


user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT 
    DISTINCT
    userId AS user_id,
    firstName AS first_name,
    lastName AS last_name,
    gender,
    level
FROM staging_events
WHERE page = 'NextSong' 
AND userId IS NOT NULL;
""")                       

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT 
    DISTINCT
    song_id,
    title,
    artist_id,
    year,
    duration
FROM staging_songs
WHERE song_id IS NOT NULL;
""")
                            
artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT 
    DISTINCT
    artist_id, 
    artist_name AS name,
    artist_location AS location,
    artist_latitude AS latitude,
    artist_longitude AS longitude
FROM staging_songs
WHERE artist_id IS NOT NULL;                                             
""")
                                    
time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT 
        ts AS start_time,
        EXTRACT(hour FROM ts) AS hour,
        EXTRACT(day FROM ts) AS day,
        EXTRACT(week FROM ts) AS week,
        EXTRACT(month FROM ts) AS month,
        EXTRACT(year FROM ts) AS year,
        EXTRACT(weekday FROM ts) AS weekday
FROM 
    staging_events
WHERE ts IS NOT NULL;
""")
    
# VERIGYING THE ROWS of DATA INSERTED IN EACH TABLE
get_number_staging_events = ("""
    SELECT COUNT(*) FROM staging_events
""")

get_number_staging_songs = ("""
    SELECT COUNT(*) FROM staging_songs
""")

get_number_songplay = ("""
    SELECT COUNT(*) FROM songplay
""")

get_number_users = ("""
    SELECT COUNT(*) FROM users
""")

get_number_songs = ("""
    SELECT COUNT(*) FROM songs
""")

get_number_artists = ("""
    SELECT COUNT(*) FROM artists
""")

get_number_time = ("""
    SELECT COUNT(*) FROM time
""")

# QUERY LISTS

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]

copy_table_order = ['staging_events', 'staging_songs']

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_order = ['artists', 'songs', 'time', 'users', 'songplay']

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

check_queries= [get_number_staging_events, get_number_staging_songs, get_number_songplay, get_number_users, get_number_songs, get_number_artists, get_number_time]

check_query_order = ['staging events', 'staging songs', 'count of songplay','count of users', 'count of songs', 'count of artists','count of time']