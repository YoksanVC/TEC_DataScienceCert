# General Imports
from pyspark.sql.functions import col, udf
from pyspark.sql.types import DateType
from library.args_parser import ArgParser
from library.read_csv import read_csv
from library.data_integrity import clean_nan, date_format
from library.data_transformation import bpm_correction

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
    df_atletas_clean.show()

    # Correcting Ritmo cardiaco in df_nadar and df_correr so data will be standardize
    df_nadar_bpm_corrected = bpm_correction(df_nadar)
    df_nadar_bpm_corrected.show()

    df_correr_bpm_corrected = bpm_correction(df_correr)
    df_correr_bpm_corrected.show()

    # Standardizing date type in both nadar and correr dataframes
    # Register function to convert date as UDF
    formats = ['%Y-%m-%d', '%d-%m-%Y', '%Y/%m/%d', '%d/%m/%Y']
    udf_date_format = udf(lambda x: date_format(x, formats), DateType())

    df_nadar_date_std = df_nadar_bpm_corrected.withColumn('Fecha', udf_date_format(col('Fecha')))
    df_nadar_date_std.show()
    df_nadar_date_std.printSchema()

    df_correr_date_std = df_correr_bpm_corrected.withColumn('Fecha', udf_date_format(col('Fecha')))
    df_correr_date_std.show()
    df_correr_date_std.printSchema()


# Read attributes from command line to store each file in a variable
if __name__ == '__main__':
    main()
