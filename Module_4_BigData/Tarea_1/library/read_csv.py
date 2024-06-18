from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

def read_csv(csv_file_name):
    spark = SparkSession.builder.appName("Read and Print CSV File").getOrCreate()

    if ("atleta" in csv_file_name):
        csv_schema = StructType([StructField('Correo_Electronico', StringType()),
                                StructField('Nombre', StringType()),
                                StructField('Pais', StringType())])
    elif("nadar" in csv_file_name):
        csv_schema = StructType([StructField('Correo_Electronico', StringType()),
                            StructField('Ritmo_Cardiaco', IntegerType()),
                            StructField('Distancia_Total_(m)', IntegerType()),
                            StructField('Total_brazadas', IntegerType()),
                            StructField('Total_de_minutos_de_actividad', IntegerType()),
                            StructField('Fecha', StringType())])
    elif("correr" in csv_file_name):
        csv_schema = StructType([StructField('Correo_Electronico', StringType()),
                            StructField('Ritmo_Cardiaco', IntegerType()),
                            StructField('Distancia_Total_(m)', IntegerType()),
                            StructField('Ganancia_de_Altura_(m)', IntegerType()),
                            StructField('Total_de_minutos_de_actividad', IntegerType()),
                            StructField('Fecha', StringType())])

    dataframe = spark.read.csv(csv_file_name,
                            schema=csv_schema,
                            header=False)

    dataframe.show()
    return dataframe