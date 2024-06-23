# General Imports
import logging
from library.yaml_parser import yaml_to_spark_df, yaml_file_parser

# Configuring logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def elt_data():
    # Parsing YAML files
    yaml_data = yaml_file_parser('./datasets/caja_12.yaml')
    print(yaml_data)

    df_yaml = yaml_to_spark_df(yaml_data)
    df_yaml.show()
    df_yaml.printSchema()

    return True

def main():
    elt_data()
    return True

# Execute MAIN
if __name__ == '__main__':
    main()