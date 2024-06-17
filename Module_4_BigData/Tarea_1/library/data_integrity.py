# Data Integrity: Function to check different data integrity problems like null/missing vales, values without sense, others.
# General imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, isnan
import datetime

def clean_nan(dataframe):
    """ Function to remove any line with NaN (missing values) and measure the amount of data lost in this process

    Args:
        dataframe (Spark DataFrame): Initial DataFrame that can contain NaN values.

    Returns:
        cleanned_dataframe: Copy of the dataframe sent without the rows that containned missing values.
    """
    initial_rows_count = dataframe.count()
    print(f"Initial count of rows before removing NaN: {initial_rows_count}")

    # Removing rows with NaN
    cleanned_dataframe = dataframe.dropna()

    # Printing cleaning results and % of data lost
    final_rows_count = cleanned_dataframe.count()
    rows_lost = (1-(final_rows_count/initial_rows_count)) * 100
    print(f"Final count of rows after removing NaN: {final_rows_count}")
    print(f"Percentage of rows lost: {round(rows_lost,2)}%")

    return cleanned_dataframe


def bpm_correction(dataframe, limit = 80):
    """ Function that corrects any Ritmo Cardiaco value below a limit bpm or NaN or Null to 120 bpm. It's not possible for someone to be doing sports running with a low hearth rate,
        so that number below 80 must be read error from hardware.

    Args:
        dataframe (Spark DataFrame): DataFrame with Ritmo Cardiaco column
        limit (int, optional): Low limit to be used as filter. Defaults to 80.

    Returns:
        corrected_df: DataFrame with all BPM below limit corrected to 120 bpm.
    """
    # Removing NaN or Null
    cleanned = dataframe.withColumn('Ritmo Cardiaco',
                                        when(isnan(col('Ritmo Cardiaco')) | col('Ritmo Cardiaco').isNull(), 120).otherwise(col('Ritmo Cardiaco')))

    # Correction BPM to 120 bpm if needed
    corrected_df = cleanned.withColumn('Ritmo Cardiaco',
                                        when(col('Ritmo Cardiaco') < limit, 120).otherwise(col('Ritmo Cardiaco')))
    
    return corrected_df


def date_format(date,formats):
    """ Function to check the date format and set it to YYY-MM-DD

    Args:
        date (Date): Date with the date to be analyzed
        formats (Array): List with accepted date formats

    Returns:
        date object: Date fixed with the right format
    """
    for fmt in formats:
        try:
            return datetime.datetime.strptime(date, fmt)
        except ValueError:
            continue
    return None