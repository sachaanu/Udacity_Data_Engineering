# etl2

import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Dat, TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Function to create spark session
    :return: the instantiated spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    """
    Description:
                Function that processes the raw data from the S3 bucket
    Parameters:
    
                :spark:       uses the earlier instantiated spark session
                :input_data:  path of the location where the files are residing
                :output_data: path of the location where the files will be saved after processing
                :return:      none
    
    """
    
    song_data = input_data + 'song_data/A/A/A/*.json'

    songSchema = R([
        Fld("artist_id",Str()),
        Fld("artist_latitude",Dbl()),
        Fld("artist_location",Str()),
        Fld("artist_longitude",Dbl()),
        Fld("artist_name",Str()),
        Fld("duration",Dbl()),
        Fld("num_songs",Int()),
        Fld("title",Str()),
        Fld("year",Int()),
    ])
    
    df = spark.read.json(song_data, schema=songSchema)

    song_fields = ["title", "artist_id","year", "duration"]

    songs_table = df.select(song_fields).dropDuplicates().withColumn("song_id", monotonically_increasing_id())
    
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet(output_data + 'songs/')

    artists_fields = ["artist_id", "artist_name as name", "artist_location as location", "artist_latitude as latitude", "artist_longitude as longitude"]

    artists_table = df.selectExpr(artists_fields).dropDuplicates()

    artists_table.write.mode('overwrite').parquet(output_data + 'artists/')


def process_log_data(spark, input_data, output_data):
    
    """
    Description:
                Function loads data from logs in Udacity S3 bucket, processess it and after extraction of artist and songs tables
                loads them into my S3 bucket. Output df from earlier functin is used to filter the page == 'NextSong'
    Parameters:
                spark       : Earlier initiated spark session
                input_data  : Udacity location of log data in json format for events
                output_data : my S3 bucket where the dimensional tables in parquet format will be written
                return      : None
    """
    
    log_data = input_data + 'log_data/*/*/*.json'

    df = spark.read.json(log_data)
    
    df = df.filter(df.page == 'NextSong')

    users_fields = ["userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level"]
    users_table = df.selectExpr(users_fields).dropDuplicates()

    users_table.write.mode('overwrite').parquet(output_data + 'users/')
    
    get_timestamp = udf(lambda x : datetime.utcfromtimestamp(int(x)/1000), TimestampType())
    df = df.withColumn("start_time", get_timestamp('ts'))

    time_table = df.withColumn("hour",hour("start_time"))\
                    .withColumn("day",dayofmonth("start_time"))\
                    .withColumn("week",weekofyear("start_time"))\
                    .withColumn("month",month("start_time"))\
                    .withColumn("year",year("start_time"))\
                    .withColumn("weekday",dayofweek("start_time"))\
                    .select("ts","start_time","hour", "day", "week", "month", "year", "weekday").drop_duplicates()
    
    time_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data + 'time/')
    
    song_df = spark.read\
                .format("parquet")\
                .option("basePath", os.path.join(output_data, "songs/"))\
                .load(os.path.join(output_data, "songs/*/*/"))

        # extract columns from joined song and log datasets to create songplays table
    songplays_table = df.join(song_df, df.song == song_df.title, how='inner')\
    .select(monotonically_increasing_id().alias("songplay_id"),col("start_time"),col("userId").alias("user_id"),"level","song_id","artist_id", col("sessionId").alias("session_id"), "location", col("userAgent")\
            .alias("user_agent"))

    songplays_table = songplays_table.join(time_table, songplays_table.start_time == time_table.start_time, how="inner")\
                        .select("songplay_id", songplays_table.start_time, "user_id", "level", "song_id", "artist_id", "session_id", "location", "user_agent", "year", "month")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.drop_duplicates().write.parquet(os.path.join(output_data, "songplays/"), mode="overwrite", partitionBy=["year","month"])

def main():
    """
        Extract songs and events data from S3, Transform it into dimensional tables format, and Load it back to S3 in Parquet format
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://bucket-sachaan/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()