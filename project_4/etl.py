import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import NullType
from pyspark.sql.types import TimestampType
from pyspark.sql.types import DoubleType
from pyspark.sql.types import LongType
from pyspark.sql.types import DateType

from pyspark.sql.functions import desc
from pyspark.sql.functions import asc

import pyspark.sql.functions as F

from pyspark.sql import Window

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ["AWS_ACCESS_KEY_ID"]= config['AWS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"]= config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    """
        Create spark session for ETL
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()

    # For use when running on local machine
    #sc=spark.sparkContext
    #hadoop_conf=sc._jsc.hadoopConfiguration()
    #hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    #hadoop_conf.set("fs.s3a.awsAccessKeyId", os.getenv('AWS_ACCESS_KEY_ID'))
    #hadoop_conf.set("fs.s3a.awsSecretAccessKey", os.getenv('AWS_SECRET_ACCESS_KEY'))

    return spark

@udf
def parseArtistLocUDF(line):
    """
        line - line of input column location, clears out hrefs and returns Null for those values
    """
    import re
    PATTERN = "^<a\shref"
    match = re.search(PATTERN, line)
    if match is None:
        return line
    else:
        return NullType()
    
def process_song_data(spark, input_data, output_data):
    """
        spark - spark session
        input_data - s3 bucket base for input data storage
        output_data - s3 bucket base for output data storage
        
        Processes song data and creates two tables artist and song for analytical queries
    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(["song_id", "title", "artist_id", "year", "duration"]).dropDuplicates()
    
    print(f"Writing to: {output_data+'song/'}...")
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(output_data+"song/songs.parquet")
    print(f"Writing to: {output_data+'song/'} completed.")
    
    
    # extract columns to create artists table
    artists_table = df.select(["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"])
    # Certain location have a href so let's get rid of those
    artists_table = artists_table.dropDuplicates().withColumn("artist_location", parseArtistLocUDF("artist_location"))
    artists_table = artists_table.withColumnRenamed("artist_name", "name")
    
    print(f"Writing to: {output_data+'artist/'}...")
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data+"artist/artists.parquet")
    print(f"Writing to: {output_data+'artist/'} completed.")
    
    # Create temp view to create songplay_table later
    songs_table.createGlobalTempView("song_table")
    artists_table.createGlobalTempView("artist_table")
    
def process_log_data(spark, input_data, output_data):
    """
        spark - spark session
        input_data - s3 bucket base for input data storage
        output_data - s3 bucket base for output data storage
        
        Processes log data and creates three tables user, time and songplay for analytical queries
    """
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # Drop the corrupted column
    df = df.drop('_corrupt_record')
    
    # filter by actions for song plays
    df = df.filter(df['page'] == 'NextSong')

    # extract columns for users table    
    users_table = df.select(["userId", "firstName", "lastName", "gender", "level", "ts"])
    
    # Create window to get latest userId status
    w1 = Window.partitionBy("userId").orderBy(F.asc("ts"))
    # Window function will number rows for the userId
    users_table = users_table.withColumn("row", F.row_number().over(w1))
    # Only pick up the latest for each userId partition
    users_table = users_table.filter(users_table["row"] == 1).drop("row", "ts")
    # Rename columns to desired values
    users_table = users_table.withColumnRenamed("userId", "user_id")\
                 .withColumnRenamed("firstName", "first_name")\
                 .withColumnRenamed("lastName", "last_name")
    
    print(f"Writing to: {output_data+'user/'}...")
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data+"user/users.parquet")
    print(f"Writing to: {output_data+'user/'} completed.")

    # create timestamp column from original timestamp column
    time_df = df.select("ts")
    
    # extract columns to create time table
    time_table = time_df.withColumn("start_time", F.to_timestamp(time_df["ts"] / 1000))\
                 .withColumn("hour", F.hour("start_time"))\
                 .withColumn("day", F.dayofmonth("start_time"))\
                 .withColumn("week", F.weekofyear("start_time"))\
                 .withColumn("month", F.month("start_time"))\
                 .withColumn("year", F.year("start_time"))\
                 .withColumn("weekday", F.dayofweek("start_time"))
    
    time_table = time_table.drop("ts").dropDuplicates()
    
    print(f"Writing to: {output_data+'time/'}...")
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data+"time/times.parquet")
    print(f"Writing to: {output_data+'time/'} completed.")

    # read in song data to use for songplays table
    song_df = df.select("ts", "userId", "level", "sessionId", "location", "userAgent", "song", "artist")
    # Convert the ts to timestamp type
    song_df = song_df.withColumn("ts", F.to_timestamp(song_df["ts"] / 1000))
    # rename columns accordingly
    song_df = song_df.withColumnRenamed("ts", "start_time")\
                     .withColumnRenamed("userId", "user_id")\
                     .withColumnRenamed("sessionId", "session_id")\
                     .withColumnRenamed("userAgent", "user_agent")
    # Create the songplay_id column
    song_df = song_df.withColumn("songplay_id", F.monotonically_increasing_id())
    
    # Create the songplay table to use sql to finish songplay table
    song_df.createGlobalTempView("songplay_table")

    # Create the artist and song table to later merge data for songplays_table
    artist_song_df = spark.sql("""
        SELECT a.artist_id, song_id, name, title
        FROM global_temp.artist_table AS a
        JOIN global_temp.song_table AS s ON a.artist_id = s.artist_id
    """)
    artist_song_df.createGlobalTempView("artist_song_table")
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
            SELECT songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
            FROM global_temp.songplay_table AS sp
            JOIN global_temp.artist_song_table AS ast ON sp.song = ast.title AND sp.artist = ast.name
    """)

    print(f"Writing to: {output_data+'songplay/'}...")
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data+"songplay/songplays.parquet")
    print(f"Writing to: {output_data+'songplay/'} completed.")


def main():
    """
        Main function for starting ETL pipeline
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-chl/"
    
    print("Begin data lake pipeline...")
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    print("Data lake pipeline completed.")


if __name__ == "__main__":
    main()
