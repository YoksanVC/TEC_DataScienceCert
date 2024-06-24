# General Imports
import os
import logging
from library.yaml_parser import yaml_to_spark_df, yaml_file_parser
from library.args_parser import yaml_files_loader
from library.data_transformation import dataframe_union

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
        if(first_file):
            first_file = False
            df_all_cashiers = df_temporal_yaml
        else:
            df_all_cashiers = dataframe_union(df_all_cashiers,df_temporal_yaml)

    # Showing Dataframe with all cashiers registered
    df_all_cashiers.show(100)
    df_all_cashiers.printSchema()

    return df_all_cashiers

def main():
    # Processing all YAML files
    df_all_sells = elt_data()

    return True

# Execute MAIN
if __name__ == '__main__':
    main()