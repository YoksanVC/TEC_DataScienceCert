# General Imports
import argparse

def ArgParser():
    # Creating a Parser
    parser = argparse.ArgumentParser(description="Processing YAML files...")

    # Creating the 3 arguments expected and parsing them
    parser.add_argument('yaml_file', type=str, help="YAML File")
    args = parser.parse_args()

    # Storing arguments
    yaml_file = args.yaml_file

    # Printing attributes that are being read
    print(f"YAML file loaded: {yaml_file}")

    # Returning attributes
    return yaml_file
