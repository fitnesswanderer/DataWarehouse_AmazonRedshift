import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
ARN = config.get("IAM_ROLE", "ARN")

# DROP TABLES

staging_events_table_drop = "DROP table if exists staging_events"
staging_songs_table_drop = "DROP table if exists staging_songs"
songplay_table_drop = "DROP table if exists songplays"
user_table_drop = "DROP table if exists users"
song_table_drop = "DROP table if exists songs"
artist_table_drop = "DROP table if exists artists"
time_table_drop = "DROP table if exists time"

# CREATE TABLES

staging_events_table_create= ("""
   CREATE TABLE IF NOT EXISTS staging_events 
     (
        artist TEXT,
        auth VARCHAR ,
        firstName VARCHAR,
        gender VARCHAR,
        itemInSession INT,
        lastName VARCHAR,
        length NUMERIC,
        level TEXT ,
        location TEXT,
        method TEXT,
        page TEXT ,
        registration NUMERIC,
        sessionId TEXT ,
        song TEXT,
        status INT,
        ts BIGINT,
        userAgent TEXT,
        userId INT
    )
   
""")

staging_songs_table_create = ("""
   CREATE TABLE IF NOT EXISTS staging_songs
    (
      num_songs INT NOT NULL 
    , artist_id TEXT  NOT NULL
    , artist_latitude FLOAT
    , artist_longitude FLOAT
    , artist_location VARCHAR
    , artist_name TEXT
    , song_id TEXT
    , title VARCHAR
    , duration FLOAT
    , year INT
    )
 
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays
    (
    songplay_id INT identity(1,1) PRIMARY KEY
    , start_time BIGINT NOT NULL  
    , user_id TEXT NOT NULL  
    , level TEXT
    , song_id TEXT  
    , artist_id TEXT    
    , session_id TEXT
    , location TEXT 
    , user_agent TEXT 
    )
""")

user_table_create = ("""
   CREATE TABLE IF NOT EXISTS users
    (
    user_id TEXT  PRIMARY KEY 
    , first_name VARCHAR 
    , last_name VARCHAR 
    , gender VARCHAR 
    , level VARCHAR 
    )
""")

song_table_create = ("""
   CREATE TABLE IF NOT EXISTS songs
    (
    song_id TEXT   PRIMARY KEY 
    , title VARCHAR  NOT NULL
    , artist_id TEXT  NOT NULL 
    , year INT NOT NULL 
    , duration FLOAT NOT NULL 
    )
""")

artist_table_create = ("""
   CREATE TABLE IF NOT EXISTS artists
    (
    artist_id TEXT   PRIMARY KEY 
    , name VARCHAR  NOT NULL
    , location VARCHAR 
    , latitude FLOAT
    , longitude FLOAT
    )
""")

time_table_create = ("""
  CREATE TABLE IF NOT EXISTS time
    (
    start_time timestamp PRIMARY KEY 
    , hour INT 
    , day INT  
    , week INT 
    , month INT  
    , year INT 
    , weekday INT 
    )
""")

# STAGING TABLES

staging_events_copy = ("""
       copy staging_events from {}
       iam_role '{}'
       format as json 's3://udacity-dend/log_json_path.json'
       region 'us-west-2';
""").format(
    config.get("S3", "LOG_DATA"),
    ARN,
    config.get("S3", "LOG_JSONPATH"))

staging_songs_copy = ("""
    copy staging_songs from {}
    iam_role '{}'
    json 'auto'
""").format(config.get("S3", "SONG_DATA"), ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (
    start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
)
SELECT DISTINCT e.ts, e.userId, e.level, s.song_id, s.artist_id, e. sessionId , e.location, e.userAgent

FROM staging_events e

JOIN staging_songs s

ON e.artist = s.artist_name

WHERE e.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)

SELECT DISTINCT(e.userId), e.firstName, e.lastName, e.gender, e.level

FROM staging_events e

WHERE e.page = 'NextSong'
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)

SELECT DISTINCT(s.song_id), s.title, s.artist_id, s.year, s.duration

FROM staging_songs s

""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)

SELECT DISTINCT(s.artist_id), s.artist_name, s.artist_location, s.artist_latitude, s.artist_longitude

FROM staging_songs s
""")

time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekday)

SELECT DISTINCT ts, EXTRACT(HOUR FROM ts), EXTRACT(DAY FROM ts), EXTRACT(WEEK FROM ts), EXTRACT(MONTH FROM ts), EXTRACT(YEAR FROM ts), EXTRACT(WEEKDAY FROM ts)

FROM(

SELECT (TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 Second ') as ts

FROM staging_events WHERE page='NextSong')
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
