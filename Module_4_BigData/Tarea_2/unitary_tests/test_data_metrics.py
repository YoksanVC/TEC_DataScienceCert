# General Imports
from library.data_metrics import max_min_sell

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

    assert max_cashier == 22
    assert min_cashier == 24