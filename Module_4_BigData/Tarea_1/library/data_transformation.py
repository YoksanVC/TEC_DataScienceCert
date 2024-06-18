# Data Transformation: Function to do different data transformations
# General imports
from pyspark.sql.functions import col

def dataframe_joiner_byEmail(dataframe1, dataframe2):
    """ Function to join two dataframes by Correo_Electronico column

    Args:
        dataframe1 (dataframe): First dataframe
        dataframe2 (dataframe): Second dataframe

    Returns:
        joint_dataframe: Joint dataframe
    """
    joint_dataframe = dataframe1.join(dataframe2, dataframe1.Correo_Electronico == dataframe2.Correo_Electronico_Atleta)

    joint_dataframe.show()
    return joint_dataframe

def keep_columns(dataframe):
    """ Funcion to keep the Correo_Electronico, Distancia_Total_(m) and Fecha columns from a given dataframe

    Args:
        dataframe (dataframe): DataFrame to be reduced

    Returns:
        reduced_df: DataFrame with the 3 columns mentioned above
    """
    reduced_df = \
    dataframe.select(
        col('Correo_Electronico').alias('Correo_Electronico_Atleta'), # Using Alias to avoid conflicts during final column selection
        col('Distancia_Total_(m)'),
        col('Fecha'))
    
    reduced_df.show()
    return reduced_df

def dataframe_union(dataframe1, dataframe2):

    if (dataframe1.columns == dataframe2.columns):
        df_concatenated = dataframe1.union(dataframe2)
        return df_concatenated
    else:
        print("Dataframes can't be concatenated, they have different columns")
        return False