import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def output_request():
    """
    Request the route to store the output files
    """
    print('Welcome to Sparkify Etl from Data Lake. Please, indicate where do you want to store the results: ')
    output = input()
    return output
    
def create_spark_session():
    """
    Creates the Spark Session.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Gets the information from the input route, transforms it and writes it as a parquet file in the output route.
    
    Keyword arguments:
    spark -- The Spark Session on which is going to be executed.
    input_data -- Route where song_data/*/*/*/*.json is stored.
    output_data -- Route where the parquet files will be stored.
    """
    
    # get filepath to song data file
    song_data = input_data + 'song_data/A/B/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id","title","artist_id","year","duration")
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("artist_id","year").parquet(output_data+'songs')

    # extract columns to create artists table
    artists_table = df.select("artist_id", col("artist_name").alias('name'), "artist_location",col("artist_latitude").alias('latitude'),col("artist_longitude").alias('longitude'))
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data+'artists')


def process_log_data(spark, input_data, output_data):
    """
    Gets the information from the input route, transforms it and writes it as a parquet file in the output route.
    
    Keyword arguments:
    spark -- The Spark Session on which is going to be executed.
    input_data -- Route where log_data/*/*/*.json is stored.
    output_data -- Route where the parquet files will be stored.
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter("page == 'NextSong'")

    # extract columns for users table    
    users_table = df.select(col("userId").alias('user_id'), col("firstName").alias('first_name'), col("lastName").alias('last_name'), "gender", "level").dropDuplicates()
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + 'users')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x/1000))))
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000)))
    df = df.withColumn('start_time', get_datetime(df.timestamp))
    
    # extract columns to create time table
    time_table =df.select("start_time") \
        .withColumn('year',year(df.start_time)) \
        .withColumn('month',month(df.start_time)) \
        .withColumn('weekofyear',weekofyear(df.start_time)) \
        .withColumn('dayofmonth',dayofmonth(df.start_time)) \
        .withColumn('hour',hour(df.start_time)) \
        .dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data + 'time')

    # read in song data to use for songplays table
    song_table = output_data + 'songs'
    artist_table = output_data + 'artists'
    song_df = spark.read.parquet(song_table)
    artist_df = spark.read.parquet(artist_table)
    temp_df = song_df.join(artist_df, song_df['artist_id'] == artist_df['artist_id'], "left").drop(artist_df['artist_id'])

    # extract columns from joined song and log datasets to create songplays table 
    df = df.join(temp_df, [df['song'] == temp_df['title'], df['artist'] == temp_df['name']], 'left')
    songplay_table = df.select(monotonically_increasing_id().alias('songplay_id'), 
                           col('start_time'), 
                           col('userId').alias('user_id'),
                           'level',
                           'song_id',
                           'artist_id', 
                           col('sessionId').alias('session_id'),
                           'location', 
                           col('userAgent').alias('user_agent'),
                           year('start_time').alias('year'),
                           month('start_time').alias('month'))


    # write songplays table to parquet files partitioned by year and month
    songplay_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data + 'songplay')


def main():
    """
    Steps performed:
    - Creates Spark Session.
    - Sets input and output path.
    - Processes the song jsons from the input, transforms them and saves them as a parquet file.
    - Processes the logs jsons from the input, transforms them and saves them as a parquet file.
    """
    input_data = "s3a://udacity-dend/"
    output_data = output_request()
    
    spark = create_spark_session()
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
