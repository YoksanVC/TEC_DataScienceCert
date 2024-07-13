# General Imports
import findspark
findspark.init('/usr/lib/python3.7/site-packages/pyspark')

import logging
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import DateType
from library.args_parser import ArgParser
from library.read_csv import read_csv
from library.data_integrity import nan_count, fill_nan_with_value, fill_nan_with_mean, date_format
from library.data_transformation import lower_case
from library.postgresql_db import save_in_db

# Configuring logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Setting up Spark session
spark = SparkSession \
    .builder \
    .appName("Video Games Spark Session") \
    .config("spark.driver.extraClassPath", "postgresql-42.7.3.jar") \
    .config("spark.executor.extraClassPath", "postgresql-42.7.3.jar") \
    .getOrCreate()

# General Function
def etl_data():
    # Parsing arguments
    vg_info_csv_file, vg_sales_csv_file = ArgParser()

    # Reading each CSV file and create its corresponding dataframe
    df_vg_info = read_csv(vg_info_csv_file, spark)
    df_vg_sales = read_csv(vg_sales_csv_file, spark)
    if(df_vg_info == False or df_vg_sales == False):
        logger.error("Error during dataframe creation, aborting execution")
        sys.exit(1) 

    # Starting cleaning process with video games info dataframe
    df_vg_info_short = df_vg_info.drop('User Ratings Count','Platforms Info', 'Developer')
    df_vg_info_short.show()
    df_vg_info_short.printSchema()

    # Counting NaN and Null
    df_vg_info_nan_null_count = nan_count(df_vg_info_short)
    df_vg_info_nan_null_count.show()

    # Fill Nan and Null in Genres, Publisher, Product Rating and User Score with "Not Specified"
    df_vg_info_nan_replaced = fill_nan_with_value(df_vg_info_short,'Genres','Not Specified')
    df_vg_info_nan_replaced = fill_nan_with_value(df_vg_info_nan_replaced,'Publisher','Not Specified')
    df_vg_info_nan_replaced = fill_nan_with_value(df_vg_info_nan_replaced,'Product Rating','Not Specified')
    df_vg_info_nan_replaced = fill_nan_with_mean(df_vg_info_nan_replaced,'User Score')
    if (df_vg_info_nan_replaced != False):
        df_vg_info_nan_replaced.show()
    else:
        logger.error("Error during filling column with Mean values, aborting")
        sys.exit(1)

    # Counting NaN and Null again
    df_vg_info_nan_null_count_2 = nan_count(df_vg_info_nan_replaced)
    df_vg_info_nan_null_count_2.show()

    # The amount of NaN is low, we can drop them at this point
    df_vg_info_cleanned = df_vg_info_nan_replaced.dropna()
    df_vg_info_cleanned.show()

    # Transforming the date to the format required if necessary
    # Register function to convert date as UDF
    formats = ['%Y-%m-%d', '%d-%m-%Y', '%Y/%m/%d', '%d/%m/%Y', '%m/%d/%Y']
    udf_date_format = udf(lambda x: date_format(x, formats), DateType())

    df_vg_info_ready = df_vg_info_cleanned.withColumn('Release Date', udf_date_format(col('Release Date')))

    # Now, clean the second dataframe with sales information
    df_vg_sales_short = df_vg_sales.drop('img','na_sales','jp_sales','pal_sales','other_sales','last_update', 'release_date', 'critic_score', 'developer')
    df_vg_sales_short.show()
    df_vg_sales_short.printSchema()

    # Counting NaN and Null
    df_vg_sales_nan_null_count = nan_count(df_vg_sales_short)
    df_vg_sales_nan_null_count.show()

    # Fill Nan and Null in Genres, Publisher, Product Rating and User Score with "Not Specified"
    df_vg_sales_nan_replaced = fill_nan_with_value(df_vg_sales_short,'console','Not Specified')
    df_vg_sales_nan_replaced = fill_nan_with_value(df_vg_sales_nan_replaced,'genre','Not Specified')
    df_vg_sales_nan_replaced = fill_nan_with_value(df_vg_sales_nan_replaced,'publisher','Not Specified')
    df_vg_sales_nan_replaced.show()

    # Counting NaN and Null again
    df_vg_sales_nan_null_count_2 = nan_count(df_vg_sales_nan_replaced)
    df_vg_sales_nan_null_count_2.show()

    # Even though there is a lot NaN in total_sales, there is not possible to generate that data, therefore, those lines are dropped
    df_vg_sales_ready = df_vg_sales_nan_replaced.dropna()

    # Returning clean dataframes
    return df_vg_info_ready, df_vg_sales_ready

# Main function
def main():
    # Creating the dataframe to be analyzed
    df_video_games_info, df_video_games_sales  = etl_data()

    # Showing Video Games Info datafrae information
    logger.info("Video Games Information Dataframe:")
    df_video_games_info.printSchema()
    df_video_games_info.show()

    # Showing Video Games Sales datafrae information
    logger.info("Video Games Sales Dataframe:")
    df_video_games_sales.printSchema()
    df_video_games_sales.show()

    # Lowering case for both dataframes
    df_video_games_info_lower = lower_case(df_video_games_info)
    df_video_games_info_lower.show()

    df_video_games_sales_lower = lower_case(df_video_games_sales)
    df_video_games_sales_lower.show()

    # Saving dataframes in POSTGRESQL DB
    vg_info_saving_status = save_in_db(df_video_games_info_lower,"video_game_info")
    vg_sales_saving_status = save_in_db(df_video_games_sales_lower,"video_game_sales")

    if(vg_info_saving_status == False or vg_sales_saving_status == False):
        logger.error("Error during saving dataframes in DB, aborting")
        sys.exit(1)

    # Successful execution, closing program
    sys.exit(0)
    

# Read attributes from command line to store each file in a variable
if __name__ == '__main__':
    main()