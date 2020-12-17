import configparser


# config
config = configparser.ConfigParser()
config.read('dwh.cfg')

# drop tables
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time"

# create the tables
staging_events_table_create= ("""
    CREATE TABLE staging_events(
        artist VARCHAR,
        auth VARCHAR,
        firstName VARCHAR,
        gender VARCHAR, 
        itemInSession VARCHAR,
        lastName VARCHAR,
        length FLOAT,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration FLOAT,
        sessionId INTEGER,
        song VARCHAR,
        status INTEGER,
        ts TIMESTAMP,
        userAgent VARCHAR,
        userId INTEGER
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs(
        num_songs INTEGER,
        artist_id VARCHAR,
        artist_latitude FLOAT,
        artist_longitude FLOAT,
        artist_location VARCHAR,
        artist_name VARCHAR,
        song_id VARCHAR,
        title VARCHAR,
        duration FLOAT,
        year INTEGER
    )
""")

songplay_table_create = ("""
    CREATE TABLE songplays(
        songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
        start_time TIMESTAMP NOT NULL SORTKEY DISTKEY,
        user_id INTEGER NOT NULL,
        level VARCHAR,
        song_id VARCHAR NOT NULL,
        artist_id VARCHAR NOT NULL,
        session_id INTEGER,
        location VARCHAR,
        user_agent VARCHAR
    )
""")

user_table_create = ("""
    CREATE TABLE users(
        user_id INTEGER NOT NULL SORTKEY PRIMARY KEY,
        first_name VARCHAR NOT NULL,
        last_name VARCHAR NOT NULL,
        gender VARCHAR NOT NULL,
        level VARCHAR NOT NULL
    )
""")

song_table_create = ("""
   CREATE TABLE songs(
       song_id VARCHAR NOT NULL SORTKEY PRIMARY KEY,
       title VARCHAR NOT NULL,
       artist_id VARCHAR NOT NULL,
       year INTEGER NOT NULL,
       duration FLOAT
    ) 
""")

artist_table_create = ("""
    CREATE TABLE artist(
        artist_id VARCHAR NOT NULL SORTKEY PRIMARY KEY,
        name VARCHAR NOT NULL,
        location VARCHAR,
        latitude FLOAT, 
        longitude FLOAT
    )
""")

time_table_create = ("""
    CREATE TABLE time(
        start_time TIMESTAMP NOT NULL DISTKEY SORTKEY PRIMARY KEY,
        hour INTEGER NOT NULL,
        day INTEGER NOT NULL,
        week INTEGER NOT NULL,
        month INTEGER NOT NULL,
        year INTEGER NOT NULL,
        weekday VARCHAR NOT NULL
    )
""")

# stage tables
staging_events_copy = ("""
    copy staging_events from {bucket}
    credentials 'aws_iam_role={arn_role}'
    region 'us-west-2' format as JSON {log_json_path} timeformat as 'epochmillisecs';
""").format(bucket=config['S3']['LOG_DATA'],
            arn_role=config['IAM_ROLE']['ARN'],
            log_json_path=config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    copy staging_songs from {bucket}
    credentials 'aws_iam_role={arn_role}'
    region 'us-west-2' format as JSON 'auto';
""").format(bucket=config['S3']['SONG_DATA'],
            arn_role=config['IAM_ROLE']['ARN'])

# final tables
songplay_table_insert = ("""
    INSERT INTO songplays (
        start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT(e.ts) AS start_time,
           e.userId AS user_id,
           e.level AS level,
           s.song_id AS song_id,
           s.artist_id AS artist_id,
           e.sessionId AS session_id,
           e.location AS location,
           e.userAgent AS user_agent
    FROM staging_events e
    JOIN staging songs s
    ON (e.song = s.title AND e.artist = s.artist_name)
    WHERE e.page == 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT(userId) AS user_id,
           firstName AS first_name,
           lastName AS last_name,
           gender, 
           level
    FROM staging_events
    WHERE user_id IS NOT NULL 
    AND page == 'NextSong';
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
