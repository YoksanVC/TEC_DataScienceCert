# Data Transformation: Function to do different data transformations
# General imports
from pyspark.sql.functions import col, when, isnan, sum, count

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
    
def product_count(dataframe):
    """ Function to aggregate by Nombre and add the amount of product per name

    Args:
        dataframe (Spark Dataframe): Dataframe to be aggregated

    Returns:
        df_aggregated_ordered: Dataframe order by Cantidad_Total
    """
    columns_to_check = ['Nombre', 'Cantidad']
    if all(column in dataframe.columns for column in columns_to_check):
        # Aggregating by Nombre
        df_aggregated = dataframe.groupBy('Nombre').agg(
            sum('Cantidad').alias('Cantidad_Total'),
        )

        # Ordering by Cantidad
        df_aggregated_ordered = df_aggregated.orderBy(df_aggregated['Cantidad_Total'].desc())

        return df_aggregated_ordered
    else:
        print("Dataframe missing Nombre and/or Cantidad columns")
        return False
    
def cashier_total_sell(dataframe):
    """ Function to calculate the amount of total cash in a cashier

    Args:
        dataframe (Spark dataframe): Dataframe to be analyzed

    Returns:
        df_aggregated_ordered: Dataframe ordered in a descending way of the amount of total cash per cashier
    """
    columns_to_check = ['Numero_Caja', 'Cantidad', 'Precio']
    if all(column in dataframe.columns for column in columns_to_check):
        # Calculate total sell per product
        df_total_sell_product = dataframe.withColumn('Total_Vendido_Producto', col('Cantidad') * col('Precio'))
        
        # Aggregating by Numero_Caja
        df_aggregated = df_total_sell_product.groupBy('Numero_Caja').agg(
            sum('Total_Vendido_Producto').alias('Total_Vendido'))

        # Ordering by Cantidad
        df_aggregated_ordered = df_aggregated.orderBy(df_aggregated['Total_Vendido'].desc())

        return df_aggregated_ordered
    else:
        print("Dataframe missing Numero_Caja, Cantidad and/or Precio columns")
        return False