from pyspark.sql.functions import col, lower
from pyspark.sql.types import StringType

def lower_case(dataframe):
    """ Function to lower case all StringType columns

    Args:
        dataframe (Spark Dataframe): Dataframe to be transformed

    Returns:
        dataframe: Dataframe with all StringType columns in lower case
    """
    for field in dataframe.schema.fields:
        if isinstance(field.dataType, StringType):
            dataframe = dataframe.withColumn(field.name, lower(col(field.name)))
    
    return dataframe

def dataframe_joiner_title(dataframe1, dataframe2):
    """ Function to join two dataframes by title column

    Args:
        dataframe1 (dataframe): First dataframe
        dataframe2 (dataframe): Second dataframe

    Returns:
        joint_dataframe: Joint dataframe
        or False: If the dataframes doesn't have title and Title columns
    """
    if('game_title' in dataframe1.columns and 'Title' in dataframe2.columns):
        joint_dataframe = dataframe1.join(dataframe2, dataframe1.game_title == dataframe2.Title)
        return joint_dataframe
    else:
        print("game_title or Title is missing")
        return False