from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, FloatType

def read_csv(csv_file_name):
    """ Function to read a CSV file and create a dataframe

    Args:
        csv_file_name (CSV file): File to be read and convert to dataframe

    Returns:
        dataframe: DataFrame created from CSV file
    """
    spark = SparkSession.builder.appName("Video Games Sales Project").getOrCreate()

    if ("all_video_games" in csv_file_name):
        csv_schema = StructType([StructField('Title', StringType()),
                                StructField('Release Date', StringType()),
                                StructField('Developer', StringType()),
                                StructField('Publisher', StringType()),
                                StructField('Genres', StringType()),
                                StructField('Product Rating', StringType()),
                                StructField('User Score', FloatType()),
                                StructField('User Ratings Count', IntegerType()),
                                StructField('Platforms Info', StringType())])
    elif("vgchartz" in csv_file_name):
        csv_schema = StructType([StructField('img', StringType()),
                                StructField('title', StringType()),
                                StructField('console', StringType()),
                                StructField('genre', StringType()),
                                StructField('publisher', StringType()),
                                StructField('developer', StringType()),
                                StructField('critic_score', FloatType()),
                                StructField('total_sales', FloatType()),
                                StructField('na_sales', FloatType()),
                                StructField('jp_sales', FloatType()),
                                StructField('pal_sales', FloatType()),
                                StructField('other_sales', FloatType()),
                                StructField('release_date', StringType()),
                                StructField('last_update', StringType())])

    # Checking if it's possible to create the dataframe
    try:
        dataframe = spark.read.csv(csv_file_name,
                                schema=csv_schema,
                                header=False)
        dataframe.printSchema()
        dataframe.show()
        return dataframe
    except Exception as ex:
        print(f"Exception: {ex}")
        return False