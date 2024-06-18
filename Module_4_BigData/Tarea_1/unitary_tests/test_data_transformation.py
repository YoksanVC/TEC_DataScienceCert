# General Librarias
from library.data_transformation import dataframe_joiner_byEmail, keep_columns, dataframe_union


def test_dataframe_joiner_byEmail(spark_session):
    """ Test the join function between the values inCorreo_Electronico column """
    df1_data = [('name2.last2@example2.com', 101), ('name3.last3@example3.com', 98), ('name1.last1@example1.com', 198)]
    df1_ds = spark_session.createDataFrame(df1_data,['Correo_Electronico', 'Ritmo Cardiaco'])
    df1_ds.show()

    # datafram2 with an extra row that will not be take into consideration, then
    df2_data = [('name1.last1@example1.com', 1000), ('name2.last2@example2.com', 2500), ('name3.last3@example3.com', 3300), ('name4.last4@example4.com', 177)]
    df2_ds = spark_session.createDataFrame(df2_data,['Correo_Electronico_Atleta', 'Distancia Total (m)'])
    df2_ds.show()

    joint_ds = dataframe_joiner_byEmail(df1_ds,df2_ds)

    expected_ds = spark_session.createDataFrame(
        [
            ('name1.last1@example1.com', 198, 'name1.last1@example1.com', 1000),
            ('name2.last2@example2.com', 101, 'name2.last2@example2.com', 2500),
            ('name3.last3@example3.com', 98, 'name3.last3@example3.com', 3300),
        ],
        ['Correo_Electronico', 'Ritmo Cardiaco', 'Correo_Electronico_Atleta', 'Distancia Total (m)'])

    expected_ds.show()
    joint_ds.show()

    assert joint_ds.collect() == expected_ds.collect()


def test_keep_important_columns(spark_session):
    """ Test to check if the right columns are selected """
    df1_data = [('name1.last1@example1.com', 101, 1234, 455, '2004-09-12'), ('name2.last2@example2.com', 98, 3344, 677, '2007-12-24'), ('name3.last3@example3.com', 198, 7689, 1024, '2023-03-06')]
    df1_ds = spark_session.createDataFrame(df1_data,['Correo_Electronico', 'Ritmo Cardiaco', 'Distancia_Total_(m)', 'Total_brazadas', 'Fecha'])
    df1_ds.show()

    reduced_ds = keep_columns(df1_ds)

    expected_ds = spark_session.createDataFrame(
        [
            ('name1.last1@example1.com', 1234, '2004-09-12'),
            ('name2.last2@example2.com', 3344, '2007-12-24'),
            ('name3.last3@example3.com', 7689, '2023-03-06'),
        ],
        ['Correo_Electronico_Atleta', 'Distancia_Total_(m)', 'Fecha'])
    
    # Fail Case
    #expected_ds = spark_session.createDataFrame(
    #    [
    #        ('name1.last1@example1.com', '2004-09-12', 1234),
    #        ('name2.last2@example2.com', '2007-12-24', 3344),
    #        ('name3.last3@example3.com', '2023-03-06', 7689),
    #    ],
    #    ['Correo_Electronico_Atleta', 'Distancia_Total_(m)', 'Fecha'])

    expected_ds.show()
    reduced_ds.show()

    assert reduced_ds.collect() == expected_ds.collect()

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