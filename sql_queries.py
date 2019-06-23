import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

"""
SQL queries to drop tales if they exist
"""
# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = 'DROP TABLE IF EXISTS "user"'
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = ("""CREATE TABLE IF NOT EXISTS staging_events
(
    se_event_id INTEGER IDENTITY(0,1),
    se_artist VARCHAR(25),
    se_auth VARCHAR(25),
    se_first_name VARCHAR(25) NOT NULL,
    se_gender VARCHAR(11),
    se_item_in_session INTEGER NOT NULL,
    se_last_name VARCHAR(25) NOT NULL,
    se_length DOUBLE PRECISION,
    se_level VARCHAR(11) NOT NULL,
    se_location VARCHAR(50),
    se_method VARCHAR(11),
    se_page VARCHAR(11),
    se_registration BIGINT,
    se_session_id INTEGER NOT NULL,
    se_song VARCHAR(50),
    se_status INTEGER,
    se_timestamp TIMESTAMP,
    se_user_agent VARCHAR(100),
    se_user_id INTEGER NOT NULL
) 
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs
(
    ss_num_songs INTEGER NOT NULL,
    ss_artist_id VARCHAR(50) NOT NULL,
    ss_artist_latitude DECIMAL,
    ss_artist_longitude DECIMAL,    
    ss_artist_location VARCHAR(50),
    ss_artist_name VARCHAR(22) NOT NULL,
    ss_song_id VARCHAR(50) NOT NULL,
    ss_title VARCHAR(25) NOT NULL,
    ss_duration DECIMAL NOT NULL,
    ss_year INTEGER NOT NULL
)
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay
(
    sp_songplay_id INTEGER NOT NULL,
    sp_start_time TIMESTAMP,
    sp_user_id INTEGER NOT NULL,
    sp_level VARCHAR(22),
    sp_song_id VARCHAR(50) NOT NULL,
    sp_artist_id VARCHAR(50) NOT NULL,
    sp_session_id INTEGER NOT NULL,
    sp_location VARCHAR(25) NOT NULL,
    sp_user_agent VARCHAR(50) NOT NULL
)
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS "user"
(
    u_user_id INTEGER NOT NULL,
    u_first_name VARCHAR(22) NOT NULL,
    u_last_name VARCHAR(22) NOT NULL,
    u_gender VARCHAR(11) NOT NULL,
    u_level VARCHAR(11) NOT NULL
)
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS song
(
    s_song_id VARCHAR(50) NOT NULL,
    s_title VARCHAR(25) NOT NULL,
    s_artist_id VARCHAR(50) NOT NULL,
    s_year INTEGER NOT NULL,
    s_duration DECIMAL NOT NULL
)
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artist
(
    a_artist_id VARCHAR(50) NOT NULL,
    a_name VARCHAR(22) NOT NULL,
    a_location VARCHAR(25) NOT NULL,
    a_latitude DECIMAL,
    a_longitude DECIMAL
)
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time
(
    t_start_time TIMESTAMP,
    t_hour INTEGER NOT NULL,
    t_day INTEGER NOT NULL,
    t_week INTEGER NOT NULL,
    t_month INTEGER NOT NULL,
    t_year INTEGER NOT NULL,
    t_weekday VARCHAR(25)
)
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {}
    iam_role {}
    region'us-west-2' FORMAT AS JSON {};
""").format(config.get("S3", "LOG_DATA"), config.get("IAM_ROLE", "ARN"), config.get("S3", "LOG_JSONPATH"))

staging_songs_copy = ("""
    copy staging_songs from {}
    iam_role {}
    region 'us-west-2' FORMAT AS JSON 'auto'
""").format(config.get("S3", "SONG_DATA"), config.get("IAM_ROLE", "ARN"))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay(sp_start_time, sp_level, sp_song_id, sp_artist_id, sp_session_id, sp_location, sp_user_agent) VALUES (%s, %s, %s, %s, %s, %s, %s);
""")

user_table_insert = ("""
INSERT INTO "user"(u_user_id, u_first_name, u_last_name, u_gender, u_level) VALUES (%s, %s, %s, %s, %s) ON CONFLICT(u_user_id) DO UPDATE u_level = EXCLUDED.u_level;
""")

song_table_insert = ("""
INSERT INTO song(s_song_id, s_title, s_artist_id, s_year, s_duration) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (s_song_id) DO NOTHING;
""")

artist_table_insert = ("""
    INSERT INTO artist(a_artist_id, a_name, a_location, a_latitude, a_longitude) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (a_artist_id) DO NOTHING;
""")

time_table_insert = ("""
INSERT INTO time(t_start_time, t_hour, t_day, t_week, t_month, t_year, t_weekday) VALUES (%s, %s, %s, %s, %s, %s, %s);
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,
                        songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop,
                      songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert,
                        song_table_insert, artist_table_insert, time_table_insert]
