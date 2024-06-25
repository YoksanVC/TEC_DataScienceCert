# General Imports
import os
import logging
import shutil
from library.yaml_parser import yaml_to_spark_df, yaml_file_parser
from library.args_parser import yaml_files_loader
from library.data_transformation import dataframe_union, product_count, cashier_total_sell

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
                old_file_name = os.path.join(tmp_output_dir, f)
                new_file_name = os.path.join(tmp_output_dir, file_name)
                os.rename(old_file_name,new_file_name)
                
                # Copy file to Result folder
                results_dir = './Results'
                final_file_name = os.path.join(results_dir, file_name)
                
                # Copy the file
                shutil.copy(new_file_name, final_file_name)
            except Exception as ex:
                print(f'Exception found: {ex}')
                return False
        else:
            continue
        
    # Remove tmp directory
    shutil.rmtree(tmp_output_dir)
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

    # Creating one CSV file
    output_dir_prod_total = './Results/tmp_prod_total'
    #df_prod_total_amount.coalesce(1).write.csv(output_dir_prod_total, header=True)
    df_prod_total_amount.write.csv(output_dir_prod_total, header=True)

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
    
    # Creating one CSV file
    output_dir_cashier_total = './Results/tmp_cashier_total'
    #df_cashier_total_amount.coalesce(1).write.csv(output_dir_cashier_total, header=True)
    df_cashier_total_amount.write.csv(output_dir_cashier_total, header=True)
    
    # Renaming file to match desire name
    cashier_total_filename = 'total_cajas.csv'
    changing_name_status = create_csv(output_dir_cashier_total, cashier_total_filename)
    if(changing_name_status == False):
        logger.error('Error changing name from CSV file, aborting')
        return False
    
    return True

# Execute MAIN
if __name__ == '__main__':
    main()