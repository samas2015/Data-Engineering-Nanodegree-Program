import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "Drop table if exists staging_events  "
staging_songs_table_drop = "Drop table if exists staging_songs"
songplay_table_drop = "Drop table if exists songplay"
user_table_drop = "Drop table if exists users"
song_table_drop = "Drop table if exists song"
artist_table_drop = "Drop table if exists artist"
time_table_drop = "Drop table if exists time"

# CREATE TABLES

staging_events_table_create= ("create table if not exists staging_events (artist varchar,auth varchar,firstName varchar,gender char(1),itemInSession int,lastName varchar,lenght numeric,level varchar,location varchar,method varchar,page varchar,registeration varchar,sessionId int, song varchar, status int, ts BIGINT,userAgent varchar,userId int) ")

staging_songs_table_create = ("create table if not exists staging_songs(num_songs int, artist_id varchar, artist_latitude varchar, artist_longitude varchar, artist_location varchar, artist_name varchar, song_id varchar, title varchar, duration numeric, year int)")

songplay_table_create = ("create table if not exists songplays (songplay_id int identity(0,1) primary key,start_time timestamp not null , user_id int not null, level varchar, song_id varchar , artist_id varchar , session_id int, location varchar, user_agent text)")

user_table_create = ("create table if not exists users (user_id int primary key, first_name varchar, last_name varchar, gender char(1), level varchar)")

song_table_create = ("create table if not exists songs (song_id varchar primary key, title varchar not null, artist_id varchar not null, year int, duration numeric not null) ")

artist_table_create = ("create table if not exists artists (artist_id varchar primary key, name varchar not null, location varchar, latitude double precision,longitude double precision )")

time_table_create = ("create table if not exists time (start_time timestamp , hour int , day int, week int, month int, year int, weekday varchar)")

add_foreign_keys="alter table songplays ADD CONSTRAINT fk_users FOREIGN KEY (user_id) REFERENCES  users(user_id),\
ADD CONSTRAINT fk_songs FOREIGN KEY(song_id) REFERENCES songs(song_id),\
ADD CONSTRAINT fk_artists FOREIGN KEY(artist_id) REFERENCES artist(artist_id);"

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {}
    credentials 'aws_iam_role={}' format as JSON  {}  compupdate off region 'us-west-2'
""".format(config['S3']['LOG_DATA'],config['IAM_ROLE']['ARN'],config['S3']['LOG_JSONPATH']))

staging_songs_copy = (""" 
    copy staging_songs from {}
    credentials 'aws_iam_role={}'  format as json 'auto' compupdate off region 'us-west-2'
""").format(config['S3']['SONG_DATA'],config['IAM_ROLE']['ARN'])

# FINAL TABLES

user_table_insert = ("insert into users (user_id,first_name,last_name,gender,level) \
                     select distinct(e.userid) as user_id, e.firstName as first_name,e.lastName as last_name,e.gender as gender,e.level as level from staging_events e where e.userid is not null")


song_table_insert = ("insert into songs (song_id,title,artist_id,year,duration) select distinct(s.song_id) as song_id,s.title as title ,s.artist_id as artist_id,s.year as year,s.duration as duration from staging_songs s ")

artist_table_insert = ("insert into artists (artist_id,name,location,latitude,longitude) select distinct(s.artist_id) as artist_id,s.artist_name as name,s.artist_location as location,s.artist_latitude::numeric as latitude,s.artist_longitude::numeric as longitude  from staging_songs s ")

time_table_insert = ("insert into time (start_time,hour,day,week,month,year,weekday) select TIMESTAMP 'epoch' + ts/1000 * interval '1 second' as start_time,EXTRACT(hour from start_time)as hour ,extract(day from start_time) as day ,extract(week from start_time) as week,extract(month from start_time) as month,extract(year from start_time) as year,extract(weekday from start_time) as weekday from staging_events e ")


songplay_table_insert = ("insert into songplays (start_time,user_id,level,song_id,artist_id,session_id,location,user_agent) select TIMESTAMP 'epoch' + ts/1000 * interval '1 second' as start_time, e.userId as user_id,e.level as level, s.song_id as song_id,s.artist_id as artist_id, e.sessionId as session_id,e.location as location  ,e.userAgent as user_agent from staging_events  e join staging_songs s on s.title=e.song and s.artist_name = e.artist where page='NextSong' ")



# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy,staging_songs_copy ]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
