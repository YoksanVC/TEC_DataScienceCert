# Data Analysis: Functions to do different data analysis with a dataframe
# General imports
from pyspark.sql.functions import col, when, isnan
from library.data_integrity import clean_nan

def top_athletes_totalDistance_perCountry(dataframe):
    # Loop to check all countries
    columns_to_check = ['Nombre', 'Pais','Distancia_Total_(m)']
    if all(column in dataframe.columns for column in columns_to_check):
        dataframe = dataframe.withColumn('Distancia_Total_(m)', when(isnan(col('Distancia_Total_(m)')), 0).otherwise(col('Distancia_Total_(m)')))
        df_country = dataframe.groupBy('Nombre','Pais').sum()
        df_country_clean = clean_nan(df_country)
        return df_country_clean
    else:
        print("Dataframe missing Nombre and/or Pais and/or Distancia_Total_(m) columns")
        return False