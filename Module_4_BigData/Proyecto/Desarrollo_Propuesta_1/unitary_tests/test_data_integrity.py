# General Libraries
from pyspark.sql.functions import col, udf, to_date
from pyspark.sql.types import DateType
from library.data_integrity import nan_count

def test_count_nan(spark_session):
    """ Test the NaN cleaning process (removing rows) """
    employee_data = [(None, 43, None), 
                    (2, None, 200),
                    (3, None, None),
                    (None, 33, None),
                    (5, None, None)]
    employee_ds = spark_session.createDataFrame(employee_data,['Employee_id', 'Age', 'Cube Number'])
    employee_ds.show()

    clean_employee_ds = nan_count(employee_ds)

    expected_ds = spark_session.createDataFrame(
        [
            (2, 3, 4),
        ],
        ['Employee_id', 'Age', 'Cube Number'])

    expected_ds.show()
    clean_employee_ds.show()

    assert clean_employee_ds.collect() == expected_ds.collect()
