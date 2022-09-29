import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format ,dayofweek
from pyspark.sql.types import TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    
    """
    load json song data files from s3 to analytics tables by spark then 
    load them back to s3 in parquet partitiioned files
    """
    
    # get filepath to song data file
    song_data = "s3a://udacity-dend/song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    df.createOrReplaceTempView("songs_data_table")
    # extract columns to create songs table
    songs_table = spark.sql(''' select DISTINCT song_id, title, artist_id, year, duration 
    from songs_data_table''')
    
    # write songs table to parquet files partitioned by year and artist
    path=output_data+"songs"
    songs_table.write.mode("overwrite").partitionBy("year","artist_id").parquet(path)

    # extract columns to create artists table
    artists_table = spark.sql('''select DISTINCT artist_id, 
    artist_name as name, 
    artist_location as location,
    artist_latitude  as latitude ,
    artist_longitude as longitude 
    from songs_data_table ''')
    
    # write artists table to parquet files
    path=output_data+"artists"
    artists_table.write.mode("overwrite").parquet(path)


def process_log_data(spark, input_data, output_data):
    """
    load json log data files from s3 to analytics tables by spark then 
    load them back to s3 in parquet partitiioned files
    """
    
    # get filepath to log data file
    log_data = "s3a://udacity-dend/log_data/*/*/*.json"

    # read log data file
    df =  spark.read.json(log_data)
    
    # create temp. view table
    df.createOrReplaceTempView("log_data_table")
    
    # filter by actions for song plays
    df = spark.sql(''' select * from log_data_table where page="NextSong" ''')

    # recreate temp. view table for filtered log
    df.createOrReplaceTempView("log_data_table")

    # extract columns for users table    
    users_table = spark.sql(''' select DISTINCT userId as user_id, 
    firstName as first_name,
    lastName as last_name, 
    gender, level from log_data_table''')
    
    # write users table to parquet files
    path=output_data+"users"
    users_table.write.mode("overwrite").parquet(path)

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000.0),TimestampType())
    #spark.udf.register("get_timestamp", lambda x: int(datetime.datetime.fromtimestamp(x / 1000.0).hour))
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x / 1000.0),TimestampType())
    df = df.withColumn('datetime', get_timestamp(df.ts))
    
    # extract columns to create time table
    df = df.withColumn("start_time", date_format(df.timestamp, 'HH:mm:ss')).\
    withColumn("hour",hour(df.timestamp)).withColumn("day", dayofmonth(df.timestamp)).\
    withColumn("week", weekofyear(df.timestamp)).withColumn("month", month(df.timestamp)).\
    withColumn("weekday", dayofweek(df.timestamp)).withColumn("year", year(df.timestamp))
    
    
    # recreate temp. view table for filtered log
    df.createOrReplaceTempView("log_data_table")
    
    time_table = spark.sql('''select DISTINCT start_time,hour,day,week,month,weekday,year from log_data_table''')
    
    # write time table to parquet files partitioned by year and month
    path=output_data+"time"
    time_table.write.partitionBy("year","month") \
        .mode("overwrite").parquet(path)

    # read in song data to use for songplays table
    path=output_data+"songs"
    song_df = spark.read.parquet(path)

    # extract columns from joined song and log datasets to create songplays table 
    song_df.createOrReplaceTempView("songs_data_table")

    songplays_table = spark.sql(''' select l.start_time, l.userId as user_id  ,
    l.level, s.song_id, s.artist_id, l.sessionId as session_id, l.location, 
    l.userAgent as user_agent,l.year,l.month  from 
    songs_data_table s join log_data_table l 
    on s.title=l.song and s.duration = l.length where page='NextSong' ''')

    # write songplays table to parquet files partitioned by year and month
    path=output_data+"songplays"
    songplays_table.write.partitionBy("year","month")\
    .mode("overwrite").parquet(path)


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://s3-udacityprj/"
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
