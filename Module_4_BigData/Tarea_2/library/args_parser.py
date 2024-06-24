# General Imports
import argparse
import os

def yaml_files_loader():
    """ Function to get a directory from arguments, and return an array with all the
        YAML files inside and the directory where they are located
    Returns:
        yaml_dir: Directory where all YAML are located
        yaml_files: Array with all YAML files names
    """
    # Creating a Parser
    parser = argparse.ArgumentParser(description="Processing YAML files directory")

    # Creating one argument expected and parsing it
    parser.add_argument('yaml_dir', type=str, help="YAML Directory")
    args = parser.parse_args()

    # Storing argument
    yaml_dir = args.yaml_dir

    # Printing attributes that are being read
    print(f"YAML directory file: {yaml_dir}")

    # Read every YAML file in directory
    yaml_files = [f for f in os.listdir(yaml_dir) if f.endswith('.yaml')]

    # Returning array with all yaml files and the directory where they are
    return yaml_dir, yaml_files
