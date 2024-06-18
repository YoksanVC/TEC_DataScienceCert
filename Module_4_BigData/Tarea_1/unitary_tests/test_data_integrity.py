# General Librarias
from pyspark.sql.functions import col, udf, to_date
from pyspark.sql.types import DateType
from library.data_integrity import clean_nan, date_format

def test_remove_rows_nan(spark_session):
    """ Test the NaN cleaning process (removing rows) """
    employee_data = [(1, 43, 100), (2, None, 200), (3, 23, 300), (4, None, None)]
    #employee_data = [(1, 43, 100), (2, 33, 200), (3, 23, 300), (4, None, 400)] # Fail Case to try
    employee_ds = spark_session.createDataFrame(employee_data,['Employee_id', 'Age', 'Cube Number'])
    employee_ds.show()

    clean_employee_ds = clean_nan(employee_ds)

    expected_ds = spark_session.createDataFrame(
        [
            (1, 43, 100),
            (3, 23, 300),
        ],
        ['Employee_id', 'Age', 'Cube Number'])

    expected_ds.show()
    clean_employee_ds.show()

    assert clean_employee_ds.collect() == expected_ds.collect()

def test_correct_date(spark_session):
    """ Test to convert the date into YYY-MM-DD"""
    control_data = [(1, "Nancy", '1988/09/23'), (2, "Marcos", '25/03/2001'), (3, "Julio", '18-11-1998'), (4, "Esteban", '2011-08-20')]
    control_ds = spark_session.createDataFrame(control_data,['ID', 'Name', 'Birth Date'])
    control_ds.show()
    
    # Create list of formats and register function to convert date as UDF
    formats = ['%Y-%m-%d', '%d-%m-%Y', '%Y/%m/%d', '%d/%m/%Y']
    udf_date_format = udf(lambda x: date_format(x, formats), DateType())

    # Apply UDF to Birth Date
    corrected_ds = control_ds.withColumn('Birth Date', udf_date_format(col('Birth Date')))

    expected_ds = spark_session.createDataFrame(
        [
            (1, "Nancy", '1988-09-23'),
            (2, "Marcos", '2001-03-25'),
            (3, "Julio", '1998-11-18'),
            (4, "Esteban", '2011-08-20'),
        ],
        ['ID', 'Name', 'Birth Date'])
    
    # Converting Birth Date to Date type for expected dataset
    expected_ds_dateType = expected_ds.withColumn('Birth Date', to_date(col('Birth Date'), 'yyyy-MM-dd'))

    expected_ds_dateType.show()
    corrected_ds.show()

    assert corrected_ds.collect() == expected_ds_dateType.collect()
