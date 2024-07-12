# General Imports
import argparse

def ArgParser():
    # Creating a Parser
    parser = argparse.ArgumentParser(description="Processing the 3 CSV files...")

    # Creating the 3 arguments expected and parsing them
    parser.add_argument('atletas_csv_file', type=str, help="Atletas CSV file")
    parser.add_argument('nadar_csv_file', type=str, help="Nadar CSV file")
    parser.add_argument('correr_csv_file', type=str, help="Correr CSV file")
    args = parser.parse_args()

    # Storing arguments
    atletas_csv = args.atletas_csv_file
    nadar_csv = args.nadar_csv_file
    correr_csv = args.correr_csv_file

    # Printing attributes that are being read
    print(f"Atletas CSV loaded: {atletas_csv}")
    print(f"Nadar CSV loaded: {nadar_csv}")
    print(f"Correr CSV loaded: {correr_csv}")
    print("CSV parsing completed!")

    # Returning attributes
    return atletas_csv, nadar_csv, correr_csv
