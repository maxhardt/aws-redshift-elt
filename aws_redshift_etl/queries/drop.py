# queries for dropping all tables

staging_events_table_drop = "drop table if exists staging.s_events"
staging_songs_table_drop = "drop table if exists staging.s_songs"
songplay_table_drop = "drop table if exists analytics.f_songplays"
user_table_drop = "drop table if exists analytics.d_users"
song_table_drop = "drop table if exists analytics.d_songs"
artist_table_drop = "drop table if exists analytics.d_artists"
time_table_drop = "drop table if exists analytics.d_time"
