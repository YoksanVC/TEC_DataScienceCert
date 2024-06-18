# Data Transformation: Function to do different data transformations
# General imports
from pyspark.sql.functions import col, when, isnan

def dataframe_joiner_byEmail(dataframe1, dataframe2):
    """ Function to join two dataframes by Correo_Electronico column

    Args:
        dataframe1 (dataframe): First dataframe
        dataframe2 (dataframe): Second dataframe

    Returns:
        joint_dataframe: Joint dataframe
        or False: If the dataframes doesn't have Correo_Electronico and Correo_Electronico_Atleta columns
    """
    if('Correo_Electronico' in dataframe1.columns and 'Correo_Electronico_Atleta' in dataframe2.columns):
        joint_dataframe = dataframe1.join(dataframe2, dataframe1.Correo_Electronico == dataframe2.Correo_Electronico_Atleta)

        joint_dataframe.show()
        return joint_dataframe
    else:
        print("Correo_Electronico or Correo_Electronico_Atleta is missing")
        return False

def keep_columns(dataframe):
    """ Funcion to keep the Correo_Electronico, Distancia_Total_(m) and Fecha columns from a given dataframe

    Args:
        dataframe (dataframe): DataFrame to be reduced

    Returns:
        reduced_df: DataFrame with the 3 columns mentioned above
        or False: If the dataframe doesn't have those columns
    """
    columns_to_check = ['Correo_Electronico', 'Distancia_Total_(m)', 'Fecha']
    if all(column in dataframe.columns for column in columns_to_check):
        reduced_df = \
        dataframe.select(
            col('Correo_Electronico').alias('Correo_Electronico_Atleta'), # Using Alias to avoid conflicts during final column selection
            col('Distancia_Total_(m)'),
            col('Fecha'))
    
        reduced_df.show()
        return reduced_df
    else:
        print("Dataframe doesn't have the right columns")
        return False

def dataframe_union(dataframe1, dataframe2):
    """ Function that concatenate two dataframes if they have the same column only

    Args:
        dataframe1 (DataFrame): First DataFrame to be united to
        dataframe2 (DataFrame): Second DataFrame to be united with

    Returns:
        df_concatenated: united DataFrame with both inputs
        or False: If the columns are not the same
    """
    if (dataframe1.columns == dataframe2.columns):
        df_concatenated = dataframe1.union(dataframe2)
        return df_concatenated
    else:
        print("Dataframes can't be concatenated, they have different columns")
        return False
    
def aggregate_by_email_date(dataframe):
    """ Function to do aggregation using Email and Date columns

    Args:
        dataframe (DataFrame): DataFrame to be aggregated

    Returns:
        df_aggregated: aggregated DataFrame
        or False: If the columns are missing
    """
    columns_to_check = ['Correo_Electronico_Atleta', 'Fecha','Distancia_Total_(m)']
    if all(column in dataframe.columns for column in columns_to_check):
        # Replacing any NaN with zeros to avoid sum issues
        dataframe_clean = dataframe.withColumn('Distancia_Total_(m)', when(isnan(col('Distancia_Total_(m)')), 0).otherwise(col('Distancia_Total_(m)')))
        df_aggregated = dataframe_clean.groupBy('Correo_Electronico_Atleta', 'Fecha').sum()
        return df_aggregated
    else:
        print("Dataframe missing Correo_Electronico_Atleta and/or Fecha and/or Distancia_Total_(m) columns")
        return False