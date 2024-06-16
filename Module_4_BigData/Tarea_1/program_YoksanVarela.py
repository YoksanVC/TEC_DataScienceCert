# General Imports
from library.args_parser import ArgParser
from library.read_csv import read_csv

# Main function
def main():
    # Parsing arguments
    atletas_csv_file, nadar_csv_file, correr_csv_file = ArgParser()

    # Reading each CSV file and create its corresponding dataframe
    df_atletas = read_csv(atletas_csv_file)
    df_nadar = read_csv(nadar_csv_file)
    df_correr = read_csv(correr_csv_file)

# Read attributes from command line to store each file in a variable
if __name__ == '__main__':
    main()
