# General Imports
import logging
import sys
from pyspark.sql.functions import col, udf, lit, row_number
from pyspark.sql.types import DateType
from pyspark.sql.window import Window
from library.args_parser import ArgParser
from library.read_csv import read_csv
from library.data_integrity import nan_count

# Configuring logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# General Function
def elt_data():
    # Parsing arguments
    vg_info_csv_file, vg_sales_file = ArgParser()

    # Reading each CSV file and create its corresponding dataframe
    df_vg_info = read_csv(vg_info_csv_file)
    df_vg_sales = read_csv(vg_sales_file)
    if(df_vg_info == False or df_vg_sales == False):
        logger.error("Error during dataframe creation, aborting execution")
        sys.exit(1) 

    # Starting cleaning process with video games info dataframe
    df_vg_info_cutted = df_vg_info.drop('User Ratings Count','Platforms Info', 'Developer')
    df_vg_info_cutted.printSchema()
    df_vg_info_cutted.show()

    # Counting NaN and Null
    df_vg_info_nan_null_count = nan_count(df_vg_info_cutted)
    df_vg_info_nan_null_count.show()

    # Return dataframe ready to be analyzed
    #return df_vg_info_ready, df_vg_sales_ready
    return True, True

# Main function
def main():
    # Creating the dataframe to be analyzed
    df_video_games_info, df_video_games_sales  = elt_data()

    # Showing Video Games Info datafrae information
    #logger.info("Video Games Information Dataframe:")
    #df_video_games_info.printSchema()
    #df_video_games_info.show()

    # Showing Video Games Sales datafrae information
    #logger.info("Video Games Sales Dataframe:")
    #df_video_games_sales.printSchema()
    #df_video_games_sales.show()

    # Successful execution, closing program
    sys.exit(0)
    

# Read attributes from command line to store each file in a variable
if __name__ == '__main__':
    main()