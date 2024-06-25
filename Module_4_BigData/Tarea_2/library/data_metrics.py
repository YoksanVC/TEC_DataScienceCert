# Data Metrics: To calculate different metrics with dataframes
# General Imports
from pyspark.sql.functions import max, min
from library.data_transformation import cashier_total_sell

def max_min_sell(dataframe):
    # Create a dataframe with total sells in money by cashier
    df_total_sells_per_cashier = cashier_total_sell(dataframe)
    df_total_sells_per_cashier.show()

    # Return cashier for max value and its cashier
    max_value = df_total_sells_per_cashier.agg(
        max('Total_Vendido').alias('max_value')).collect()[0]['max_value']
    df_max = df_total_sells_per_cashier.filter(df_total_sells_per_cashier['Total_Vendido'] == max_value)
    max_cashier = df_max.select('Numero_caja').collect()[0]['Numero_caja']
    
    # Return cashier for min value
    min_value = df_total_sells_per_cashier.agg(
        min('Total_Vendido').alias('min_value')).collect()[0]['min_value']
    df_min = df_total_sells_per_cashier.filter(df_total_sells_per_cashier['Total_Vendido'] == min_value)
    min_cashier = df_min.select('Numero_caja').collect()[0]['Numero_caja']
    
    return max_cashier, min_cashier