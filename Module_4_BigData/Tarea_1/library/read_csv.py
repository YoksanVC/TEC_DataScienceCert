from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

def read_csv(csv_file_name):
    spark = SparkSession.builder.appName("Read and Print CSV File").getOrCreate()

    if ("atleta" in csv_file_name):
        csv_schema = StructType([StructField('Correo Electronico', StringType()),
                                StructField('Nombre', StringType()),
                                StructField('Pais', StringType())])
    elif("nadar" in csv_file_name):
        csv_schema = StructType([StructField('Correo Electronico', StringType()),
                            StructField('Ritmo Cardiaco', IntegerType()),
                            StructField('Distancia Total (m)', IntegerType()),
                            StructField('Total brazadas', IntegerType()),
                            StructField('Total de minutos de actividad', IntegerType()),
                            StructField('Fecha', StringType())])
    elif("correr" in csv_file_name):
        csv_schema = StructType([StructField('Correo Electronico', StringType()),
                            StructField('Ritmo Cardiaco', IntegerType()),
                            StructField('Distancia Total (m)', IntegerType()),
                            StructField('Ganancia de Altura (m)', IntegerType()),
                            StructField('Total de minutos de actividad', IntegerType()),
                            StructField('Fecha', StringType())])

    dataframe = spark.read.csv(csv_file_name,
                            schema=csv_schema,
                            header=False)

    dataframe.show()
    return dataframe