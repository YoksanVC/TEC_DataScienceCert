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