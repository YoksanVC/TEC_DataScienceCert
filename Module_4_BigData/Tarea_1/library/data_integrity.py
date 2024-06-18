# Data Integrity: Function to check different data integrity problems like null/missing vales, values without sense, others.
# General imports
import datetime

def clean_nan(dataframe):
    """ Function to remove any line with NaN (missing values) and measure the amount of data lost in this process

    Args:
        dataframe (Spark DataFrame): Initial DataFrame that can contain NaN values.

    Returns:
        cleanned_dataframe: Copy of the dataframe sent without the rows that containned missing values.
    """
    initial_rows_count = dataframe.count()
    print(f"Initial count of rows before removing NaN: {initial_rows_count}")

    # Removing rows with NaN
    cleanned_dataframe = dataframe.dropna()

    # Printing cleaning results and % of data lost
    final_rows_count = cleanned_dataframe.count()
    rows_lost = (1-(final_rows_count/initial_rows_count)) * 100
    print(f"Final count of rows after removing NaN or Null: {final_rows_count}")
    print(f"Percentage of rows lost: {round(rows_lost,2)}%")

    cleanned_dataframe.show()
    return cleanned_dataframe


def date_format(date,formats):
    """ Function to check the date format and set it to YYY-MM-DD

    Args:
        date (Date): Date with the date to be analyzed
        formats (Array): List with accepted date formats

    Returns:
        date object: Date fixed with the right format
    """
    for fmt in formats:
        try:
            return datetime.datetime.strptime(date, fmt)
        except ValueError:
            continue
    return None