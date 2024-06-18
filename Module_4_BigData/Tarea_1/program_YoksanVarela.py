# General Imports
from pyspark.sql.functions import col, udf, lit
from pyspark.sql.types import DateType
from library.args_parser import ArgParser
from library.read_csv import read_csv
from library.data_integrity import clean_nan, date_format
from library.data_transformation import dataframe_joiner_byEmail, keep_columns, dataframe_union, aggregate_by_email_date

# Main function
def main():
    # Parsing arguments
    atletas_csv_file, nadar_csv_file, correr_csv_file = ArgParser()

    # Reading each CSV file and create its corresponding dataframe
    df_atletas = read_csv(atletas_csv_file)
    df_nadar = read_csv(nadar_csv_file)
    df_correr = read_csv(correr_csv_file)

    # Removing any NaN or null value on df_atletas because there is no way to recover lost data on this data frame, and it's important for upcoming joints
    df_atletas_clean = clean_nan(df_atletas)

    # Keeping the only columns that are needed: Correo_Electronico, Distancia_Total_(m), y Fecha
    df_nadar_reduced = keep_columns(df_nadar)
    df_correr_reduced = keep_columns(df_correr)

    # Standardizing date type in both nadar and correr dataframes
    # Register function to convert date as UDF
    formats = ['%Y-%m-%d', '%d-%m-%Y', '%Y/%m/%d', '%d/%m/%Y']
    udf_date_format = udf(lambda x: date_format(x, formats), DateType())

    df_nadar_date_std = df_nadar_reduced.withColumn('Fecha', udf_date_format(col('Fecha')))
    df_nadar_date_std.show()
    df_nadar_date_std.printSchema()

    df_correr_date_std = df_correr_reduced.withColumn('Fecha', udf_date_format(col('Fecha')))
    df_correr_date_std.show()
    df_correr_date_std.printSchema()
    
    # Aggregating both nadar and correr dataframes to remove duplicated based on email and date
    df_nadar_aggregated = aggregate_by_email_date(df_nadar_date_std)
    if(df_nadar_aggregated != False):
        df_nadar_aggr = \
            df_nadar_aggregated.select(
            col('Correo_Electronico_Atleta'),
            col('sum(Distancia_Total_(m))').alias('Distancia_Total_(m)'),
            col('Fecha'))
        df_nadar_aggr.show()
        df_nadar_aggr.printSchema()
    else:
        print("Error during aggregation with email and date, aborting")
        return False
    
    df_correr_aggregated = aggregate_by_email_date(df_correr_date_std)
    if(df_correr_aggregated != False):
        df_correr_aggr = \
            df_correr_aggregated.select(
            col('Correo_Electronico_Atleta'),
            col('sum(Distancia_Total_(m))').alias('Distancia_Total_(m)'),
            col('Fecha'))
        df_correr_aggr.show()
        df_correr_aggr.printSchema()
    else:
        print("Error during aggregation with email and date, aborting")
        return False

    # Adding the corresponding sport to each dataframe
    df_nadar_with_sport = df_nadar_aggr.withColumn('Deporte', lit('Nadar'))
    df_nadar_with_sport.show()
    df_nadar_with_sport.printSchema()

    df_correr_with_sport = df_correr_aggr.withColumn('Deporte', lit('Correr'))
    df_correr_with_sport.show()
    df_correr_with_sport.printSchema()

    # Join df_atletas_clean with each df for nadar and correr
    df_partial_join_nadar = dataframe_joiner_byEmail(df_atletas_clean,df_nadar_with_sport)
    if(df_partial_join_nadar == False):
        print("Error during dataframes join, aborting")
        return False
    
    df_partial_join_correr = dataframe_joiner_byEmail(df_atletas_clean,df_correr_with_sport)
    if(df_partial_join_correr == False):
        print("Error during dataframes join, aborting")
        return False

    # Concatenate both partial df
    df_sports_contact = dataframe_union(df_partial_join_nadar, df_partial_join_correr)
    if(df_sports_contact != False):
        df_sports_contact.show(200)
        df_sports_contact.printSchema()
    else:
        print("Error during dataframes union, aborting")
        return False
    
    # Reordering columns
    df_activities = \
        df_sports_contact.select(
        col('Correo_Electronico'),
        col('Nombre'),
        col('Pais'),
        col('Deporte').alias('Actividad'),
        col('Distancia_Total_(m)'),
        col('Fecha'))
    
    df_activities.show(200)
    df_activities.printSchema()
    
    # Removing NaN or Null from df_activities
    df_activities_clean = clean_nan(df_activities)
    
    # Sorting dataframe using Fecha, in ascending order
    df_activities_date_sorted = df_activities_clean.orderBy(df_activities_clean['Fecha'].asc())
    df_activities_date_sorted.show(200)
    df_activities_date_sorted.printSchema()


# Read attributes from command line to store each file in a variable
if __name__ == '__main__':
    main()