Sparkify is a startup company that owns a music streaming app 
It wants to analyze data by detecting the current playing song and related info

How to run Python scripts:
1-Open terminal
2-Type python filename.py
In case there opened jupytar notebooks please restart kernal first

Project files:
1-data folder: contains json songs and log source files
2-sql_queries.py: python file where postgresql tables are dropped and then created
it also contains tables data insert and songplay select statement
3-create_tables: use sql_queries file to create table and this file has to be run at the beginning 
4-etl.ipynb: jupytar notebook that contains instruction and code for building a pipleline for our data model
5-etl.py: python file to process all files in data folder using our pipleline
6- test.ipynb: jupytar notebook for testing the qulaity of the data model by checking keys, data types and constraints

sequence of run project:
1-run create_tables.py
2-run etl.py
3-run test.ipynb

Database design:
-Data schema is star schema where songplays is a fact table and users,songs,artists and time are dimensional tables
ETL pipeline:
1-load song json files
2-populate songs and artists tables
3-load log json files
4-convert timestamp column to datetime
5-populate users and time tables
6-get songid and artistid from songs and artists tables by joining tables and searhing by song title and artist name and song duration
7-insert result songid and artist id with the rest of related data to songplays table

Examples of analytics queries:
-get location of paid users
select distinct location from songplays where level='paid'
result=there're 20 distict locations from total 63 where there're paid users

-search in songplays table where song_id isn't empty
SELECT * FROM songplays where song_id <> 'None'
result=there's only 1 row where song_id and artist_id aren't None
which means that there's a need to more matching data between log and songs files