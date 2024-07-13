# General Libraries
from pyspark.sql.functions import col, udf, to_date
from pyspark.sql.types import DateType, StructType, StructField, IntegerType, DoubleType, FloatType, StringType
from library.data_integrity import nan_count, fill_nan_with_value, fill_nan_with_mean, date_format

def test_count_nan(spark_session):
    """ Test the NaN counting process """
    employee_data = [(None, 43, None), 
                    (2, None, 200),
                    (3, None, None),
                    (None, 33, None),
                    (5, None, None)]
    employee_ds = spark_session.createDataFrame(employee_data,['Employee_id', 'Age', 'Cube Number'])

    clean_employee_ds = nan_count(employee_ds)

    expected_ds = spark_session.createDataFrame(
        [
            (2, 3, 4),
        ],
        ['Employee_id', 'Age', 'Cube Number'])

    expected_ds.show()
    clean_employee_ds.show()

    assert clean_employee_ds.collect() == expected_ds.collect()


def test_fill_nan_with_integer(spark_session):
    """ Test to replace NaN or Null with an specified format: Integer """
    employee_data = [(None, 43, None), 
                    (2, None, 'Finance'),
                    (3, None, None),
                    (None, 33, None),
                    (5, None, None)]
    employee_ds = spark_session.createDataFrame(employee_data,['Employee_id', 'Age', 'Department'])

    corrected_ds = fill_nan_with_value(employee_ds,'Employee_id',1)

    expected_ds = spark_session.createDataFrame(
        [
            (1, 43, None),
            (2, None, 'Finance'),
            (3, None, None),
            (1, 33, None),
            (5, None, None),
        ],
        ['Employee_id', 'Age', 'Department'])

    expected_ds.show()
    corrected_ds.show()

    assert corrected_ds.collect() == expected_ds.collect()


def test_fill_nan_with_string(spark_session):
    """ Test to replace NaN or Null with an specified format: String """
    employee_data = [(None, 43, None), 
                    (2, None, 'Finance'),
                    (3, None, None),
                    (None, 33, None),
                    (5, None, None)]
    employee_ds = spark_session.createDataFrame(employee_data,['Employee_id', 'Age', 'Department'])

    corrected_ds = fill_nan_with_value(employee_ds,'Department','Engineering')

    expected_ds = spark_session.createDataFrame(
        [
            (None, 43, 'Engineering'),
            (2, None, 'Finance'),
            (3, None, 'Engineering'),
            (None, 33, 'Engineering'),
            (5, None, 'Engineering'),
        ],
        ['Employee_id', 'Age', 'Department'])

    expected_ds.show()
    corrected_ds.show()

    assert corrected_ds.collect() == expected_ds.collect()

def test_fill_nan_with_mean(spark_session):
    """ Test to replace NaN or Null with column's mean """
    employee_data = [(None, 43, None, 250.45), 
                    (2, None, 'Finance', 546.77),
                    (3, None, None, None),
                    (None, 33, None, 344.19),
                    (5, None, None, None)]
    
    employee_schema = StructType([StructField('Employee_id', IntegerType(), True),
                                  StructField('Age', IntegerType(), True),
                                  StructField('Department', StringType(), True),
                                  StructField('Salary', DoubleType(), True)])
    
    employee_ds = spark_session.createDataFrame(employee_data,employee_schema)

    corrected_ds = fill_nan_with_mean(employee_ds,'Age')
    corrected_ds = fill_nan_with_mean(corrected_ds,'Salary')

    expected_ds = spark_session.createDataFrame(
        [
            (None, 43, None, 250.45),
            (2, 38, 'Finance', 546.77),
            (3, 38, None, 380.47),
            (None, 33, None, 344.19),
            (5, 38, None, 380.47),
        ],
        ['Employee_id', 'Age', 'Department', 'Salary'])

    expected_ds.show()
    corrected_ds.show()

    assert corrected_ds.collect() == expected_ds.collect()

def test_fill_nan_non_numerical(spark_session):
    """ Test failure case for a column that is not numerical """
    employee_data = [(None, 43, None, '250.45'), 
                    (2, None, 'Finance', '546.77'),
                    (3, None, None, None),
                    (None, 33, None, '344.19'),
                    (5, None, None, None)]
    
    employee_schema = StructType([StructField('Employee_id', IntegerType(), True),
                                  StructField('Age', IntegerType(), True),
                                  StructField('Department', StringType(), True),
                                  StructField('Salary', StringType(), True)])
    
    employee_ds = spark_session.createDataFrame(employee_data,employee_schema)

    corrected_ds = fill_nan_with_mean(employee_ds,'Salary')

    assert corrected_ds == False

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
