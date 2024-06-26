# General Imports
from library.data_metrics import max_min_sell, most_sold_product, most_profit_by_product, percentiles

def test_max_values(spark_session):
    """ Test for max values and its cashier """
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
    
    max_cashier, min_cashier = max_min_sell(df_ds)

    assert max_cashier == '22'
    assert min_cashier == '24'

def test_max_values_missing_column(spark_session):
    """ Test failure case for missing columns """
    df_data = [(20, 1, 1, 'Aguacate', 8),
               (20, 1, 2, 'Sandia', 1),
               (22, 1, 1, 'Jocote', 20),
               (22, 2, 1, 'Sandia', 3),
               (24, 1, 1, 'Aguacate', 2)]
    df_ds = spark_session.createDataFrame(df_data,['Numero_Caja', 
                                                   'Numero_Compra', 
                                                   'Numero_Producto', 
                                                   'Nombre',
                                                   'Cantidad'])
    
    max_cashier, min_cashier = max_min_sell(df_ds)

    assert max_cashier == False
    assert min_cashier == False

def test_most_sold_product(spark_session):
    """ Test for most sold products """
    df_data = [(20, 1, 1, 'Aguacate', 8, 550),
               (20, 1, 2, 'Sandia', 1, 1200),
               (22, 1, 1, 'Jocote', 20, 200),
               (22, 2, 1, 'Sandia', 3, 1200),
               (24, 1, 1, 'Jocote', 15, 200)]
    df_ds = spark_session.createDataFrame(df_data,['Numero_Caja', 
                                                   'Numero_Compra', 
                                                   'Numero_Producto', 
                                                   'Nombre',
                                                   'Cantidad',
                                                   'Precio'])
    
    most_product = most_sold_product(df_ds)

    assert most_product == 'Jocote'

def test_most_sold_product_missing_column(spark_session):
    """ Test failure case for missing columns """
    df_data = [(20, 1, 1, 'Aguacate', 550),
               (20, 1, 2, 'Sandia', 1200),
               (22, 1, 1, 'Jocote', 200),
               (22, 2, 1, 'Sandia', 1200),
               (24, 1, 1, 'Jocote', 200)]
    df_ds = spark_session.createDataFrame(df_data,['Numero_Caja', 
                                                   'Numero_Compra', 
                                                   'Numero_Producto', 
                                                   'Nombre',
                                                   'Precio'])
    
    most_product = most_sold_product(df_ds)

    assert most_product == False

def test_most_profit(spark_session):
    """ Test for max values and its cashier """
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
    
    most_profit = most_profit_by_product(df_ds)

    assert most_profit == 'Aguacate'

def test_most_profit_missing_column(spark_session):
    """ Test failure case for missing columns """
    df_data = [(20, 1, 1, 'Aguacate', 550),
               (20, 1, 2, 'Sandia', 1200),
               (22, 1, 1, 'Jocote', 200),
               (22, 2, 1, 'Sandia', 1200),
               (24, 1, 1, 'Jocote', 200)]
    df_ds = spark_session.createDataFrame(df_data,['Numero_Caja', 
                                                   'Numero_Compra', 
                                                   'Numero_Producto', 
                                                   'Nombre',
                                                   'Precio'])
    
    most_profit = most_profit_by_product(df_ds)

    assert most_profit == False

def test_percentile_calculation(spark_session):
    """ Test percentile calculations """
    df_data = [(20, 1, 1, 'Aguacate', 3, 550),
               (23, 1, 2, 'Sandia', 2, 1200),
               (21, 1, 1, 'Jocote', 15, 200),
               (23, 2, 1, 'Sandia', 4, 1200),
               (25, 1, 2, 'Sandia', 3, 1200),
               (26, 1, 1, 'Jocote', 18, 200),
               (27, 2, 1, 'Sandia', 2, 1200),
               (27, 1, 2, 'Sandia', 1, 1200),
               (28, 1, 1, 'Jocote', 12, 200),
               (22, 2, 1, 'Sandia', 1, 1200),
               (22, 2, 1, 'Aguacate', 4, 550),
               (24, 1, 1, 'Aguacate', 2, 550)]
    df_ds = spark_session.createDataFrame(df_data,['Numero_Caja', 
                                                   'Numero_Compra', 
                                                   'Numero_Producto', 
                                                   'Nombre',
                                                   'Cantidad',
                                                   'Precio'])
    
    percentile_25, percentile_50, percentile_75 = percentiles(df_ds)

    assert percentile_25 == '1650'
    assert percentile_50 == '2400'
    assert percentile_75 == '3600'

def test_percentile_calculation_missing_column(spark_session):
    """ Test failure case for missing columns """
    df_data = [(20, 1, 1, 'Aguacate', 3),
               (23, 1, 2, 'Sandia', 2),
               (21, 1, 1, 'Jocote', 15),
               (23, 2, 1, 'Sandia', 4),
               (25, 1, 2, 'Sandia', 3),
               (26, 1, 1, 'Jocote', 18),
               (27, 2, 1, 'Sandia', 2),
               (27, 1, 2, 'Sandia', 1),
               (28, 1, 1, 'Jocote', 12),
               (22, 2, 1, 'Sandia', 1),
               (22, 2, 1, 'Aguacate', 4),
               (24, 1, 1, 'Aguacate', 2)]
    df_ds = spark_session.createDataFrame(df_data,['Numero_Caja', 
                                                   'Numero_Compra', 
                                                   'Numero_Producto', 
                                                   'Nombre',
                                                   'Cantidad'])
    
    percentile_25, percentile_50, percentile_75 = percentiles(df_ds)

    assert percentile_25 == False
    assert percentile_50 == False
    assert percentile_75 == False