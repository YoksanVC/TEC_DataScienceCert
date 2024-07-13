# General Imports
import argparse

def ArgParser():
    # Creating a Parser
    parser = argparse.ArgumentParser(description="Processing the 2 CSV files...")

    # Creating the 3 arguments expected and parsing them
    parser.add_argument('videogames_info_csv_file', type=str, help="Video Game Info CSV file")
    parser.add_argument('videogames_sales_csv_file', type=str, help="Video Game Sales CSV file")
    args = parser.parse_args()

    # Storing arguments
    vg_info_csv = args.videogames_info_csv_file
    vg_sales_csv = args.videogames_sales_csv_file

    # Printing attributes that are being read
    print(f"Atletas CSV loaded: {vg_info_csv}")
    print(f"Nadar CSV loaded: {vg_sales_csv}")
    print("CSV parsing completed!")

    # Returning attributes
    return vg_info_csv, vg_sales_csv
