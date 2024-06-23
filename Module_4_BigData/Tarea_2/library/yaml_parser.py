# Functions to parse YAML files
# General Imports
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# Initialize Spark session
spark = SparkSession.builder.appName("YAML to Spark DataFrame Converter").getOrCreate()

def yaml_file_parser(yaml_file):
    """ Converts YAML files into structured data object

    Args:
        yaml_file (YAML file): YAML file to be used

    Returns:
        data: Structured YAML data object
    """
    with open (yaml_file, 'r') as file:
        data = yaml.safe_load(file)
    return data

def yaml_to_spark_df(yaml_data):
    """ Takes a YAML data object and generates a Spark DF from it

    Args:
        yaml_data (string): YAML data object

    Returns:
        dataframe: Spark Dataframe
    """
    # Creating schema for dataframe
    csv_schema = StructType([StructField('Numero_Caja', IntegerType()),
                             StructField('Numero_Compra', IntegerType()),
                             StructField('Numero_Producto', IntegerType()),
                             StructField('Nombre', StringType()),
                             StructField('Cantidad', IntegerType()),
                             StructField('Precio', IntegerType())])
    
    # Numero de caja
    tmp_name = str(yaml_data[0])
    tmp_split = tmp_name.split(':')
    numero_caja = int(tmp_split[1].strip('}'))

    # Create Row objects that can be interpreted by Spark if they can be converted to a dictionary
    df_rows = []
    dictionary = dict(yaml_data[1])
    compras_counter = 0
    for compras_totales in dictionary['compras']:
        prod_counter = 0
        compras_counter += 1
        for compra in compras_totales['compra']:
            for producto in compra['producto']:
                prod_counter += 1
                nombre = producto['nombre']
                cantidad = producto['cantidad']
                precio_unitario = producto['precio_unitario']
                df_rows.append([numero_caja, 
                                compras_counter,
                                prod_counter,
                                nombre, cantidad,
                                precio_unitario])
    
    dataframe = spark.createDataFrame(df_rows,csv_schema)
    return dataframe
