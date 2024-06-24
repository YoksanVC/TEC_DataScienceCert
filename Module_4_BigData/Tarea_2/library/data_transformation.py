# Data Transformation: Function to do different data transformations
# General imports

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