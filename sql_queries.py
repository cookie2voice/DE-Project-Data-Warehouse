import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

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
    CREATE TABLE staging_events (
        artist text distkey,
        auth varchar(30),
        firstName varchar(50),
        gender varchar(10),
        itemInSession integer,
        lastName varchar(50),
        length float,
        level varchar(10),
        location varchar(30),
        method varchar(10),
        page varchar(30),
        registration float,
        sessionId integer,
        song text,
        status integer,
        ts bigint sortkey,
        userAgent varchar(50),
        userId integer
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs (
        num_songs integer sortkey,
        artist_id text distkey,
        artist_latitude float,
        artist_longitude float,
        artist_location varchar(30),
        artist_name text,
        song_id text,
        title text,
        duration float,
        year integer
    )
""")

songplay_table_create = ("""
    CREATE TABLE songplays (
        songplay_id int identity(0,1) PRIMARY KEY,
        start_time bigint NOT NULL sortkey,
        user_id integer REFERENCES users (user_id) NOT NULL,
        level varchar(10),
        song_id text,
        artist_id text REFERENCES artists (artist_id),
        session_id integer,
        location varchar(30),
        user_agent varchar(50)
    );
""")

user_table_create = ("""
    CREATE TABLE users (
        user_id integer PRIMARY KEY sortkey,
        first_name varchar(50),
        last_name varchar(50),
        gender varchar(10),
        level varchar(10)
    );
""")

song_table_create = ("""
    CREATE TABLE songs (
        id int identity(0,1) PRIMARY KEY,
        song_id text,
        title text NOT NULL,
        artist_id text,
        year integer,
        duration float NOT NULL sortkey
    );
""")

artist_table_create = ("""
    CREATE TABLE artists (
        artist_id text PRIMARY KEY sortkey,
        name text NOT NULL,
        location text,
        latitude float,
        longitude float
    );
""")

time_table_create = ("""
    CREATE TABLE time (
        start_time timestamp PRIMARY KEY sortkey,
        hour integer,
        day integer,
        week integer,
        month integer,
        year integer,
        weekday integer
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {}
    credentials 'aws_iam_role={}'
    compupdate off region 'us-west-2' FORMAT AS JSON {}
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(config['S3'].get('LOG_DATA'),
            config['IAM_ROLE'].get('ARN').strip("'"),
            config['S3'].get('LOG_JSONPATH'))

staging_songs_copy = ("""
    copy staging_songs from {}
    credentials 'aws_iam_role={}'
    compupdate off region 'us-west-2' FORMAT AS JSON 'auto'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(config['S3'].get('SONG_DATA'),
            config['IAM_ROLE'].get('ARN').strip("'"))

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        SELECT se.ts, se.userId, se.level, ss.song_id,
            ss.artist_id, se.sessionId, se.location,
            se.userAgent
        FROM staging_events se
        JOIN staging_songs ss
         ON (se.artist = ss.artist_name)
         AND (se.song = ss.title)
         AND (se.length = ss.duration)
         WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT userId, firstName, lastName, gender, level
      FROM staging_events
      WHERE page='NextSong'
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id, title, artist_id, year, duration
    FROM staging_songs
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id, artist_name,
        artist_location, artist_latitude,
        artist_longitude
    FROM staging_songs
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT t.start_time,
    EXTRACT (HOUR FROM t.start_time), EXTRACT (DAY FROM t.start_time),
    EXTRACT (WEEK FROM t.start_time), EXTRACT (MONTH FROM t.start_time),
    EXTRACT (YEAR FROM t.start_time), EXTRACT (WEEKDAY FROM t.start_time)
    FROM
       (SELECT TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time
         FROM staging_events) t
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,
                        user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop,
                      songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert,
                        song_table_insert, artist_table_insert, time_table_insert]
