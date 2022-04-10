import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config.get('IAM_ROLE', 'ARN')
LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_events (
                artist          text,
                auth            text,
                firstName       text,
                gender          text,
                itemInSession   text,
                lastName        text,
                length          text,
                level           text,
                location        text,
                method          text,
                page            text,
                registration    text,
                sessionId       int not null sortkey distkey,
                song            text,
                status          int,
                ts              text not null,
                userAgent       text,
                userId          int
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
                song_id         text,
                artist_id       text sortkey distkey,
                artist_latitude float,
                artist_longitude float,
                artist_location text,
                artist_name     text,
                title           text,
                duration        float,
                num_songs       int,
                year            int
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
                songplay_id     int identity(1,1) sortkey,
                start_time      timestamp not null,
                user_id         int not null distkey,
                level           text not null,
                song_id         text not null,
                artist_id       text not null,
                session_id      text not null,
                location        text,
                user_agent      text
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
                user_id         int not null sortkey,
                first_name      text,
                last_name       text,
                gender          text,
                level           text
    ) diststyle all;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
                song_id         text not null sortkey,
                title           text not null,
                artist_id       text not null,
                year            int not null,
                duration        decimal not null
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
                artist_id       text not null sortkey,
                name            varchar(500),
                location        varchar(500),
                latitude        decimal,
                longitude       decimal
    ) diststyle all;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
                start_time      timestamp not null sortkey,
                hour            smallint,
                day             smallint,
                week            smallint,
                month           smallint,
                year            smallint,
                weekday         smallint
    ) diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    credentials 'aws_iam_role={}'
    format as json {}
    STATUPDATE ON
    region 'us-west-2';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    credentials 'aws_iam_role={}'
    format as json 'auto'
    ACCEPTINVCHARS AS '^'
    STATUPDATE ON
    region 'us-west-2';
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, 
                          session_id, location, user_agent)
    SELECT TIMESTAMP 'epoch' + se.ts / 1000 * INTERVAL '1 second',
            se.UserId, se.level, ss.song_id, ss.artist_id, se.sessionId, 
            se.location, se.userAgent
    FROM staging_events AS se
    JOIN staging_songs AS ss
        ON se.artist = ss.artist_name
    WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT se.userId, se.firstName, se.lastName, se.gender, se.level
    FROM staging_events AS se
    WHERE se.page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT ss.song_id, ss.title, ss.artist_id, ss.year, ss.duration
    FROM staging_songs AS ss
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT ss.artist_id, ss.artist_name, ss.artist_location, 
            ss.artist_latitude, ss.artist_longitude
    FROM staging_songs AS ss
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT TIMESTAMP 'epoch' + se.ts / 1000 * INTERVAL '1 second' AS start_time,
            EXTRACT(hour FROM start_time),
            EXTRACT(day FROM start_time),
            EXTRACT(week FROM start_time),
            EXTRACT(month FROM start_time),
            EXTRACT(year FROM start_time),
            EXTRACT(week FROM start_time)
    FROM staging_events AS se
    WHERE se.page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create,
                        user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert,
                        time_table_insert]
