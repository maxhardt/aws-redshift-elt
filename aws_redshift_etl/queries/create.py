# queries for creating schemas and tables

# staging and analytics schema

staging_schema = "create schema if not exists staging"
analytics_schema = "create schema if not exists analytics"

# staging tables

staging_events_table_create = """
create table staging.s_events (
    artist varchar (256),
    auth varchar (128),
    firstname varchar (128),
    gender varchar (10),
    iteminsession integer,
    lastname varchar (128),
    length float,
    level varchar (128),
    location varchar (128),
    method varchar (10),
    page varchar (128),
    registration bigint,
    sessionid integer,
    song varchar (256),
    status integer,
    ts bigint,
    useragent varchar (256),
    userid bigint
)
diststyle all
"""

staging_songs_table_create = """
create table if not exists staging.s_songs (
    song_id varchar (128),
    num_songs integer,
    title varchar (512),
    artist_name varchar (512),
    year integer,
    duration float,
    artist_id varchar(128),
    artist_longitude float,
    artist_latitude float,
    artist_location varchar(512)
)
distkey (artist_name)
"""

# analytics tables

songplay_table_create = """
create table if not exists analytics.f_songplays (
    songplay_id integer identity(0,1) primary key,
    start_time timestamp not null,
    user_id bigint not null,
    level varchar (128),
    song_id varchar (128),
    artist_id varchar (128),
    session_id bigint,
    location varchar (512),
    user_agent varchar (512),
foreign key (song_id) references analytics.d_songs(song_id),
foreign key (artist_id) references analytics.d_artists(artist_id),
foreign key (user_id) references analytics.d_users(user_id),
foreign key (start_time) references analytics.d_time(start_time)
)
diststyle even
"""

user_table_create = """
create table if not exists analytics.d_users (
    user_id integer primary key,
    first_name varchar (128) not null,
    last_name varchar (128) not null,
    gender varchar (128) not null,
    level varchar (128)
)
distkey (user_id)
"""

song_table_create = """
create table if not exists analytics.d_songs (
	song_id varchar (128) primary key,
	title varchar (512) not null,
	artist_id varchar (128) not null,
	year smallint,
	duration float not null,
	foreign key (artist_id) references analytics.d_artists
)
distkey (song_id)
"""

artist_table_create = """
create table if not exists analytics.d_artists (
    artist_id varchar (128) primary key,
    name varchar (512) not null,
    location varchar (512),
    latitude float,
    longitude float
)
distkey(artist_id)
"""

time_table_create = """
create table analytics.d_time (
	start_time timestamp primary key,
	hour smallint not null,
	day smallint not null,
	week smallint not null,
	month smallint not null,
	year smallint not null,
	weekday smallint not null
)
distkey (start_time)
"""
