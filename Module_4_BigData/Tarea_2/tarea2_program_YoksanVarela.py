# General Imports
import os
import logging
from library.yaml_parser import yaml_to_spark_df, yaml_file_parser
from library.args_parser import yaml_files_loader
from library.data_transformation import dataframe_union, product_count

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
                    logger.info('Error during DF union, aborting')
                    return False
        else:
            logger.info('Error during DF creation from YAML, aborting')
            return False

    # Showing Dataframe with all cashiers registered
    df_all_cashiers.show(100)
    df_all_cashiers.printSchema()

    return df_all_cashiers

def main():
    # Processing all YAML files
    df_all_sells = elt_data()
    if(df_all_sells == False):
        logger.info('Error during ELT stage, aborting')
        return False

    # Clean NaN and Nulls
    df_all_sells_cleanned = df_all_sells.dropna()

    # First CSV: Total amount per product
    df_prod_total_amount = product_count(df_all_sells_cleanned)

    if(df_prod_total_amount != False):
        df_prod_total_amount.show()
        df_prod_total_amount.printSchema()
    else:
        logger.info('Error during aggregation, aborting')
        return False

    # Creating one CSV file
    output_dir = './Results'
    df_prod_total_amount.coalesce(1).write.csv(output_dir, header=True)

    # Renaming file to match desire name
    total_prod_filename = 'total_productos.csv'
    for f in os.listdir(output_dir):
        if('part-' in f):
            old_file_name = os.path.join(output_dir, f)
            new_file_name = os.path.join(output_dir, total_prod_filename)
            os.rename(old_file_name,new_file_name)
        elif('total_productos' in f):
            continue

    return True

# Execute MAIN
if __name__ == '__main__':
    main()