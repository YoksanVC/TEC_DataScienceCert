# Data Integrity: Function to check different data integrity problems like null/missing vales, values without sense, others.
# General imports
import datetime
from pyspark.sql.functions import col, sum, mean
from pyspark.sql.types import IntegerType, FloatType, DoubleType

def nan_count(dataframe):
    """ Function to count all NaN and Null present in each dataframe column

    Args:
        dataframe (Spark DataFrame): Initial DataFrame that can contain NaN values.

    Returns:
        df_nan_count: Dataframe with the count of NaN and Null in the dataset
    """
    df_nan_count = dataframe.select([sum(col(c).isNull().cast("int")).alias(c) for c in dataframe.columns])
    return df_nan_count

def fill_nan_with_value(dataframe,column,value):
    """ Function to fill NaN in a specific column with a specific value

    Args:
        dataframe (Spark Dataframe): _description_
        column (_type_): Column where the NaN or Null will be replace
        value (_type_): Value to replace the NaN or Null with

    Returns:
        df_corrected: Spark dataframe with the NaN replaced in the specified column
    """
    df_corrected = dataframe.fillna({column:value})
    return df_corrected

def fill_nan_with_mean(dataframe,column):
    """ Function to fill NaN in a specific column with its mean value, only if the column is a numerical type

    Args:
        dataframe (Spark Dataframe): _description_
        column (int, float or double): Column where the NaN or Null will be replace

    Returns:
        df_corrected: Spark dataframe with the NaN replaced in the specified column
    """
    # Checking if columns are numerical types
    if (any(isinstance(field.dataType, IntegerType) for field in dataframe.schema.fields if field.name == column) or
        any(isinstance(field.dataType, FloatType) for field in dataframe.schema.fields if field.name == column) or
        any(isinstance(field.dataType, DoubleType) for field in dataframe.schema.fields if field.name == column)):
        # Calculating the mean for all the numeric columns pass by
        mean_value = dataframe.agg(mean(column)).first()[0]

        # Create a dictionary with the mean per column
        df_corrected = dataframe.fillna({column:mean_value})
        return df_corrected
    else:
        print('At least one of the columns is not numerical type')
        return False

def date_format(date,formats):
    """ Function to check the date format and set it to YYYY-MM-DD

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