from aws_redshift_etl.queries.drop import *
from aws_redshift_etl.queries.create import *
from aws_redshift_etl.queries.ingest import *
from aws_redshift_etl.queries.transform import *

drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    song_table_drop,
    artist_table_drop,
    user_table_drop,
    time_table_drop
]

drop_table_queries_no_staging = [
    songplay_table_drop,
    song_table_drop,
    artist_table_drop,
    user_table_drop,
    time_table_drop
]

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    time_table_create,
    user_table_create,
    artist_table_create,
    song_table_create,
    songplay_table_create
]

copy_table_queries = [
    staging_events_copy,
    staging_songs_copy
]

insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert
]
