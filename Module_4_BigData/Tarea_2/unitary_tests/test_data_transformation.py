# General Libraries
from library.data_transformation import dataframe_union, product_count, cashier_total_sell


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

def test_product_count_correct_sum(spark_session):
    """ Test the product count with a correct dataframe """
    df_data = [(20, 1, 1, 'Aguacate', 8, 550),
               (20, 1, 2, 'Sandia', 1, 1200),
               (22, 1, 1, 'Jocote', 20, 200),
               (22, 2, 1, 'Sandia', 3, 1200),
               (24, 1, 1, 'Aguacate', 2, 550)]
    df_ds = spark_session.createDataFrame(df_data,['Numero_Caja', 
                                                   'Numero_Compra', 
                                                   'Numero_Producto', 
                                                   'Nombre',
                                                   'Cantidad',
                                                   'Precio'])
    
    df_aggregated = product_count(df_ds)

    expected_ds = spark_session.createDataFrame(
        [
            ('Jocote', 20),
            ('Aguacate', 10),
            ('Sandia', 4),
        ],
        ['Nombre', 'Cantidad_Total'])
    
    expected_ds.show()
    df_aggregated.show()

    assert df_aggregated.collect() == expected_ds.collect()

def test_product_count_missing_column(spark_session):
    """ Test failure mode with a critical column is missing """
    df_data = [(20, 1, 1, 'Aguacate', 550),
               (20, 1, 2, 'Sandia', 1200),
               (22, 1, 1, 'Jocote', 200),
               (22, 2, 1, 'Sandia', 1200),
               (24, 1, 1, 'Aguacate', 550)]
    df_ds = spark_session.createDataFrame(df_data,['Numero_Caja', 
                                                   'Numero_Compra', 
                                                   'Numero_Producto', 
                                                   'Nombre', 
                                                   'Precio'])
    
    df_aggregated = product_count(df_ds)

    assert df_aggregated == False
    
def test_cashier_total_sell_correct_sum(spark_session):
    """ Test the correct sum per cashier """
    df_data = [(20, 1, 1, 'Aguacate', 8, 550),
               (20, 1, 2, 'Sandia', 1, 1200),
               (22, 1, 1, 'Jocote', 20, 200),
               (22, 2, 1, 'Sandia', 3, 1200),
               (24, 1, 1, 'Aguacate', 2, 550)]
    df_ds = spark_session.createDataFrame(df_data,['Numero_Caja', 
                                                   'Numero_Compra', 
                                                   'Numero_Producto', 
                                                   'Nombre',
                                                   'Cantidad',
                                                   'Precio'])
    
    df_aggregated = cashier_total_sell(df_ds)

    expected_ds = spark_session.createDataFrame(
        [
            (22, 7600),
            (20, 5600),
            (24, 1100),
        ],
        ['Numero_Caja', 'Total_Vendido'])
    
    expected_ds.show()
    df_aggregated.show()

    assert df_aggregated.collect() == expected_ds.collect()
    
def test_cashier_total_sell_missing_column(spark_session):
    """ Test failure mode with a critical column is missing """
    df_data = [(20, 1, 1, 'Aguacate', 550),
               (20, 1, 2, 'Sandia', 1200),
               (22, 1, 1, 'Jocote', 200),
               (22, 2, 1, 'Sandia', 1200),
               (24, 1, 1, 'Aguacate', 550)]
    df_ds = spark_session.createDataFrame(df_data,['Numero_Caja', 
                                                   'Numero_Compra', 
                                                   'Numero_Producto', 
                                                   'Nombre', 
                                                   'Precio'])
    
    df_aggregated = product_count(df_ds)

    assert df_aggregated == False