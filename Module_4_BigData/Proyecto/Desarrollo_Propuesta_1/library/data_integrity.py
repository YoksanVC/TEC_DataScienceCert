# Data Integrity: Function to check different data integrity problems like null/missing vales, values without sense, others.
# General imports
import datetime
from pyspark.sql.functions import col, sum

def nan_count(dataframe):
    """ Function to count all NaN and Null present in each dataframe column

    Args:
        dataframe (Spark DataFrame): Initial DataFrame that can contain NaN values.

    Returns:
        df_nan_count: Dataframe with the count of NaN and Null in the dataset
    """
    df_nan_count = dataframe.select([sum(col(c).isNull().cast("int")).alias(c) for c in dataframe.columns])
    df_nan_count.show()

    return df_nan_count


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