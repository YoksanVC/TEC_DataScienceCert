# General Imports
import os
import logging
import shutil
from pyspark.sql import SparkSession
from library.yaml_parser import yaml_to_spark_df, yaml_file_parser
from library.args_parser import yaml_files_loader
from library.data_transformation import dataframe_union, product_count, cashier_total_sell
from library.data_metrics import max_min_sell, most_sold_product, most_profit_by_product, percentiles

# Create spark session
spark = SparkSession.builder.appName("Main Code").getOrCreate()

# Configuring logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def elt_data():
    # Parsing YAML files and directory
    yaml_directoy, yaml_files = yaml_files_loader()
    print(yaml_files)

    # Create only one dataframe. Using flag to start union after first file proccessed
    first_file = True
    for file in yaml_files:
        # Create a YAML object with each file
        yaml_path = os.path.join(yaml_directoy, file)
        yaml_data = yaml_file_parser(yaml_path)

        # Create a Spark DF with each YAML object
        df_temporal_yaml = yaml_to_spark_df(yaml_data)

        if(df_temporal_yaml != False):
            if(first_file):
                first_file = False
                df_all_cashiers = df_temporal_yaml
            else:
                df_all_cashiers = dataframe_union(df_all_cashiers,df_temporal_yaml)
                if(df_all_cashiers == False):
                    logger.error('Error during DF union, aborting')
                    return False
        else:
            logger.error('Error during DF creation from YAML, aborting')
            return False

    # Showing Dataframe with all cashiers registered
    df_all_cashiers.show(100)
    df_all_cashiers.printSchema()

    return df_all_cashiers

def create_csv(tmp_output_dir,file_name):
    """ Function to change the CSV created by pySpark to a desire name

    Args:
        tmp_output_dir (directory): Temporal directory created to save CSV by pySpark
        file_name (string): Desire name for CSV

    Returns:
        True/False: Boolean to indicate successful/error condition
    """
    for f in os.listdir(tmp_output_dir):
        if('part-' in f):
            try:
                results_dir = './Results'
                old_file_name = os.path.join(tmp_output_dir, f)
                new_file_name = os.path.join(results_dir, file_name)
                shutil.copy(old_file_name,new_file_name)
            except Exception as ex:
                print(f'Exception found: {ex}')
                return False
        else:
            continue
        
    # Remove tmp directory
    return True

def main():
    # Processing all YAML files
    df_all_sells = elt_data()
    if(df_all_sells == False):
        logger.error('Error during ELT stage, aborting')
        return False

    # Clean NaN and Nulls
    df_all_sells_cleanned = df_all_sells.dropna()

    # First CSV: Total amount per product
    df_prod_total_amount = product_count(df_all_sells_cleanned)

    if(df_prod_total_amount != False):
        df_prod_total_amount.show(100)
        df_prod_total_amount.printSchema()
    else:
        logger.error('Error during aggregation, aborting')
        return False

    # Creating CSV file
    output_dir_prod_total = './Results/tmp_prod_total'
    df_prod_total_amount.coalesce(1).write.csv(output_dir_prod_total, header=True)

    # Renaming file to match desire name
    total_prod_filename = 'total_productos.csv'
    changing_name_status = create_csv(output_dir_prod_total, total_prod_filename)
    if(changing_name_status == False):
        logger.error('Error changing name from CSV file, aborting')
        return False

    # Second CSV: Total sold by cashier
    df_cashier_total_amount = cashier_total_sell(df_all_sells_cleanned)

    if(df_cashier_total_amount != False):
        df_cashier_total_amount.show()
        df_cashier_total_amount.printSchema()
    else:
        logger.error('Error during aggregation, aborting')
        return False
    
    # Creating CSV file
    output_dir_cashier_total = './Results/tmp_cashier_total'
    df_cashier_total_amount.coalesce(1).write.csv(output_dir_cashier_total, header=True)
    
    # Renaming file to match desire name
    cashier_total_filename = 'total_cajas.csv'
    changing_name_status = create_csv(output_dir_cashier_total, cashier_total_filename)
    if(changing_name_status == False):
        logger.error('Error changing name from CSV file, aborting')
        return False
    
    # Third CSV: Metrics
    header = ['Tipo de Metrica', 'Valor']
    metrics_data = []

    # Cashier with most/less sells
    max_cashier, min_cashier = max_min_sell(df_all_sells_cleanned)
    if(max_cashier == False or min_cashier == False):
        logger.error('Error during metric calculation, aborting')
        return False
    metrics_data.append(['caja_con_mas_ventas',max_cashier])
    metrics_data.append(['caja_con_menos_ventas',min_cashier])

    # Percentiles 25, 50 and 75
    percentile_25, percentile_50, percentile_75 = percentiles(df_all_sells_cleanned)
    if(percentile_25 == False or percentile_50 == False or percentile_75 == False):
        logger.error('Error during metric calculation, aborting')
        return False
    metrics_data.append(['percentile_25_por_caja',percentile_25])
    metrics_data.append(['percentile_50_por_caja',percentile_50])
    metrics_data.append(['percentile_75_por_caja',percentile_75])

    # Most sold product
    top_product_sold = most_sold_product(df_all_sells_cleanned)
    if(top_product_sold == False):
        logger.error('Error during metric calculation, aborting')
        return False
    metrics_data.append(['producto_mas_vendido_por_unidad',top_product_sold])

    # Most profit by product
    top_profit_product = most_profit_by_product(df_all_sells_cleanned)
    if(top_profit_product == False):
        logger.error('Error during metric calculation, aborting')
        return False
    metrics_data.append(['producto_de_mayor_ingreso',top_profit_product])

    # Putting together the dataframe
    df_metrics = spark.createDataFrame(metrics_data, header)
    df_metrics.show()
    df_metrics.printSchema()

    # Creating CSV file
    output_dir_metrics = './Results/tmp_metrics'
    df_metrics.coalesce(1).write.csv(output_dir_metrics, header=True)
    
    # Renaming file to match desire name
    metrics_filename = 'metricas.csv'
    changing_name_status = create_csv(output_dir_metrics, metrics_filename)
    if(changing_name_status == False):
        logger.error('Error changing name from CSV file, aborting')
        return False
    
    return True

# Execute MAIN
if __name__ == '__main__':
    main()