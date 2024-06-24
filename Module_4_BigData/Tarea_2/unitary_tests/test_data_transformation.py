# General Libraries
from library.data_transformation import dataframe_union


def test_concatenate_same_columns(spark_session):
    """ Test to concatenate two dataframes that have the same columns """
    df1_data = [(111, 222, 333), (777, 888, 999)]
    df1_ds = spark_session.createDataFrame(df1_data,['col1', 'col2', 'col3'])
    df1_ds.show()

    df2_data = [(333, 222, 111), (999, 888, 777)]
    df2_ds = spark_session.createDataFrame(df2_data,['col1', 'col2', 'col3'])
    df2_ds.show()

    concatenated_ds = dataframe_union(df1_ds,df2_ds)

    expected_ds = spark_session.createDataFrame(
        [
            (111, 222, 333),
            (777, 888, 999),
            (333, 222, 111),
            (999, 888, 777),
        ],
        ['col1', 'col2', 'col3'])
    
    expected_ds.show()
    concatenated_ds.show()

    assert concatenated_ds.collect() == expected_ds.collect()

def test_concatenate_different_columns(spark_session):
    """ Test for failure mode when columns are not the same """
    df1_data = [(111, 222, 333), (777, 888, 999)]
    df1_ds = spark_session.createDataFrame(df1_data,['col1', 'col2', 'col3'])
    df1_ds.show()

    df2_data = [(333, 222, 111), (999, 888, 777)]
    df2_ds = spark_session.createDataFrame(df2_data,['col4', 'col2', 'col3'])
    df2_ds.show()

    concatenated_ds = dataframe_union(df1_ds,df2_ds)

    assert concatenated_ds == False