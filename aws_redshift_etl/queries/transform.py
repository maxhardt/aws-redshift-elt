# queries for inserting data from staging to analytics schema

songplay_table_insert = """
insert into analytics.f_songplays (
	start_time,
	user_id,
	level,
	song_id,
	artist_id,
	session_id,
	location,
	user_agent
)
select
	timestamp 'epoch' + e.ts / 1000 * interval '1 second' as start_time,
    e.userid as user_id,
    e.level,
    s.song_id,
    s.artist_id,
    e.sessionid as session_id,
    e.location,
    e.useragent as user_agent
from staging.s_events as e
left join (
		select
			any_value(d.song_id) as song_id,
			any_value(d.artist_id) as artist_id,
  			any_value(d.duration) as duration,
			d.artist_name,
			d.title
		from (
			select distinct *
			from staging.s_songs
		) as d
		group by d.title, d.artist_name
    ) as s
    on e.artist = s.artist_name and e.song = s.title
where e.page = 'NextSong'
"""

user_table_insert = """
insert into analytics.d_users
select
	w.userid,
    w.firstname,
    w.lastname,
    w.level,
    w.gender
from (
	select
		userid,
		level,
		row_number() over(partition by userid order by ts desc) as rank
	from staging.s_events
	where page = 'NextSong'
) as w
where rank = 1
"""

song_table_insert = """
insert into analytics.d_songs
select
    g.song_id,
    g.title,
    g.artist_id,
    g.duration,
    g.year
from (
	select
  		d.title,
  		d.duration,
  		any_value(d.song_id) as song_id,
  		any_value(d.artist_id) as artist_id,
  		any_value(d.year) as year
    from (
		select distinct *
		from staging.s_songs
		) as d
  	group by d.title, d.artist_name, d.duration
	) as g
"""

artist_table_insert = """
insert into analytics.d_artists
select
	d.artist_id,
	any_value(d.artist_name) as name,
	any_value(d.artist_location) as location,
	any_value(d.artist_latitude) as latitude,
	any_value(d.artist_longitude) as longitude
from (
	select distinct *
	from staging.s_songs
) as d
group by d.artist_id
"""

time_table_insert = """
insert into analytics.d_time
select
  	fg.start_time,
	extract(hour from start_time) as hour,
	extract(day from start_time) as day,
	extract(week from start_time) as week,
	extract(month from start_time) as month,
	extract(year from start_time) as year,
	extract(weekday from start_time) as weekday
from (
	select f.start_time
	from analytics.f_songplays as f
	where f.start_time is not null
	group by f.start_time
) as fg
"""
