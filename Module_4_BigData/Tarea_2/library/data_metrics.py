# Data Metrics: To calculate different metrics with dataframes
# General Imports
from pyspark.sql.functions import max, min, sum, col
from library.data_transformation import cashier_total_sell

def max_min_sell(dataframe):
    """ Function that returns the cashier with the max and min money by selling

    Args:
        dataframe (Spark Dataframe): Dataframe to be analyzed

    Returns:
        max_cashier, min_cashier: String of the Cashier ID for max and min selling
    """
    # Create a dataframe with total sells in money by cashier
    df_total_sells_per_cashier = cashier_total_sell(dataframe)

    # Return cashier for max value and its cashier
    if (df_total_sells_per_cashier != False):
        max_value = df_total_sells_per_cashier.agg(
            max('Total_Vendido').alias('max_value')).collect()[0]['max_value']
        df_max = df_total_sells_per_cashier.filter(df_total_sells_per_cashier['Total_Vendido'] == max_value)
        max_cashier = df_max.select('Numero_caja').collect()[0]['Numero_caja']
        
        # Return cashier for min value
        min_value = df_total_sells_per_cashier.agg(
            min('Total_Vendido').alias('min_value')).collect()[0]['min_value']
        df_min = df_total_sells_per_cashier.filter(df_total_sells_per_cashier['Total_Vendido'] == min_value)
        min_cashier = df_min.select('Numero_caja').collect()[0]['Numero_caja']
        
        return str(max_cashier), str(min_cashier)
    else:
        print('Dataframe generated with errors, aborting')
        return False, False

def most_sold_product(dataframe):
    """ Function that returns the most sold product

    Args:
        dataframe (Spark Dataframe): _description_

    Returns:
        product_name: Product that was sell the most
    """
    columns_to_check = ['Numero_Caja', 'Cantidad']
    if all(column in dataframe.columns for column in columns_to_check):
        # Aggregating by Nombre and adding the quantities
        df_aggregated = dataframe.groupBy('Nombre').agg(
                sum('Cantidad').alias('Cantidad_Total'))
        
        # Ordering dataframe in a descending way
        df_aggregated_ordered = df_aggregated.orderBy(df_aggregated['Cantidad_Total'].desc())

        # Return the product Nombre
        product_name = df_aggregated_ordered.select('Nombre').collect()[0]['Nombre']
        return product_name
    else:
        print("Dataframe missing Nombre and/or Cantidad columns")
        return False

def most_profit_by_product(dataframe):
    """ Function to calculate the most profitable product

    Args:
        dataframe (Spark Dataframe): Dataframe to be analyzed

    Returns:
        product_more_profit: Name of the product that generated the most profit
    """
    columns_to_check = ['Nombre', 'Cantidad', 'Precio']
    if all(column in dataframe.columns for column in columns_to_check):
        # Adding a new column with the math of Cantidad * Precio
        df_total_sell_product = dataframe.withColumn('Total_Vendido_Producto', 
                                                     col('Cantidad') * col('Precio'))
        
        # Aggregating by Numero_Caja
        df_aggregated = df_total_sell_product.groupBy('Nombre').agg(
            sum('Total_Vendido_Producto').alias('Total_Vendido'))

        # Ordering by Cantidad
        df_aggregated_ordered = df_aggregated.orderBy(df_aggregated['Total_Vendido'].desc())

        # Returning the product name that generated more profit
        product_more_profit = df_aggregated_ordered.select('Nombre').collect()[0]['Nombre']
        return product_more_profit
    else:
        print("Dataframe missing Nombre, Cantidad and/or Precio columns")
        return False
    
def percentiles(dataframe):
    columns_to_check = ['Nombre', 'Cantidad', 'Precio']
    if all(column in dataframe.columns for column in columns_to_check):
        # Adding a new column with the math of Cantidad * Precio
        df_total_sell_product = dataframe.withColumn('Total_Vendido_Producto', 
                                                     col('Cantidad') * col('Precio'))

        # Ordering by Total_Vendido ascending
        df_total_sell_product_ordered = df_total_sell_product.orderBy(
            df_total_sell_product['Total_Vendido_Producto'].asc())

        # Get the total amount of row and divide it by 4 to get the integer that represents 25% of data
        quater = int(df_total_sell_product_ordered.count()/4)

        # Getting the amount of money for percentile 25, 50 and 75
        perc_25 = df_total_sell_product_ordered.select(
            'Total_Vendido_Producto').collect()[quater]['Total_Vendido_Producto']
        perc_50 = df_total_sell_product_ordered.select(
            'Total_Vendido_Producto').collect()[quater * 2]['Total_Vendido_Producto']
        perc_75 = df_total_sell_product_ordered.select(
            'Total_Vendido_Producto').collect()[quater * 3]['Total_Vendido_Producto']

        return str(perc_25), str(perc_50), str(perc_75)
    else:
        print("Dataframe missing Nombre, Cantidad and/or Precio columns")
        return False,False,False