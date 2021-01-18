import os
import configparser
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, monotonically_increasing_id, to_date
from pyspark.sql.functions import year, month, dayofmonth, dayofweek, hour, weekofyear, date_format
from pyspark.sql.types import (StructType, StructField as Fld, DoubleType as Dbl, StringType as Str,
                               IntegerType as Int, DateType as Date, TimestampType as Tstamp)


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config.get['AWS', 'AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get['AWS', 'AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):

    song_data = input_data + 'song_data/*/*/*/*.json'

    # create our structure for spark to read
    song_schema = StructType([
        Fld('artist_id', Str()),
        Fld('artist_latitude', Dbl()),
        Fld('artist_longitude', Dbl()),
        Fld('artist_location', Str()),
        Fld('artist_name', Str()),
        Fld('title', Str()),
        Fld('duration', Dbl()),
        Fld('year', Int()),
        Fld('num_songs', Int())
    ])

    # read in our song data
    songs_df = spark.read.json(song_data, schema=song_schema)

    # extract columns to create songs table
    songs_fields = ['title', 'artist_id', 'duration', 'year']
    # add 'song_id' column, will generate monotonically increasing 64-bit integers
    songs_table = songs_df.select(songs_fields).dropDuplicates().withColumn(
        'song_id', monotonically_increasing_id())
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist').parquet(f'{output_data}songs/')

    # extract columns to create artists table
    artists_fields = ['artist_id', 'artist_name as name', 'artist_location as location',
                     'artist_latitude as latitude', 'artist_longitude as longitude']
    artists_table = songs_df.select(artists_fields).dropDuplicates()

    # write artists table to parquet files
    artists_table.write.parquet(f'{output_data}artists/')


def process_log_data(spark, input_data, output_data):

    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    log_df = spark.read.json(log_data)
    
    # filter by actions for song plays
    log_df = log_df.filter(log_df.page == 'NextSong')

    # extract columns for users table
    users_fields = ['userId as user_id', 'firstName as first_name', 'lastName as last_name', 'gender', 'level']
    users_table = log_df.select(users_fields).dropDuplicates() # selectExpr
    
    # write users table to parquet files
    users_table.write.parquet(f'{output_data}users/')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000), Tstamp())
    log_df = log_df.withColumn('time_stamp', get_timestamp('ts'))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: to_date(x), Tstamp())
    log_df = log_df.withColumn('start_time', get_datetime('ts'))
    
    # extract columns to create time table
    log_df = log_df.withColumn('hour', hour('time_stamp'))
    log_df = log_df.withColumn('day', dayofmonth('time_stamp'))
    log_df = log_df.withColumn('week', weekofyear('time_stamp'))
    log_df = log_df.withColumn('month', month('time_stamp'))
    log_df = log_df.withColumn('year', year('time_stamp'))
    log_df = log_df.withColumn('weekday', dayofweek('time_stamp'))

    time_fields = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_table = log_df.select('time_fields').distinct()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(f'{output_data}time/')

    # read in song data to use for songplays table
    songs_df = spark.read.parquet(f'{output_data}songs/*/*/*')

    # extract columns from song and log datasets to create songplays table
    songplays_table = log_df.join(songs_df, (log_df.artist == songs_df.artist_name)) \
        .distinct \
        .select('start_time', 'user_id', 'level', 'song_id', 'artist_id', 'session_id',
                'location', 'user_agent') \
        .withColumn('songplay_id', monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
