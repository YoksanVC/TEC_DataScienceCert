# Data Transformation: Function to do different data transformations
# General imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, isnan


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