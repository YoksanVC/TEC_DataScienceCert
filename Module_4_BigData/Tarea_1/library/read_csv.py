from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType

def read_atletas():
    spark = SparkSession.builder.appName("Read and Print - Atletas CSV File").getOrCreate()

    csv_schema = StructType([StructField('Correo Electronico', StringType()),
                            StructField('Nombre', StringType()),
                            StructField('Pais', StringType())])

    dataframe = spark.read.csv("./datasets/atletas.csv",
                            schema=csv_schema,
                            header=False)

    dataframe.show()
    return dataframe


def read_nadar():
    spark = SparkSession.builder.appName("Read and Print - Nadar CSV File").getOrCreate()

    csv_schema = StructType([StructField('Correo Electronico', StringType()),
                            StructField('Ritmo Cardiaco', IntegerType()),
                            StructField('Distancia Total (m)', IntegerType()),
                            StructField('Total brazadas', IntegerType()),
                            StructField('Total de minutos de actividad', IntegerType()),
                            StructField('Fecha', DateType())])

    dataframe = spark.read.csv("./datasets/nadar.csv",
                            schema=csv_schema,
                            header=False)

    dataframe.show()
    return dataframe


def read_correr():
    spark = SparkSession.builder.appName("Read and Print - Correr CSV File").getOrCreate()

    csv_schema = StructType([StructField('Correo Electronico', StringType()),
                            StructField('Ritmo Cardiaco', IntegerType()),
                            StructField('Distancia Total (m)', IntegerType()),
                            StructField('Ganancia de Altura (m)', IntegerType()),
                            StructField('Total de minutos de actividad', IntegerType()),
                            StructField('Fecha', DateType())])

    dataframe = spark.read.csv("./datasets/correr.csv",
                            schema=csv_schema,
                            header=False)

    dataframe.show()
    return dataframe