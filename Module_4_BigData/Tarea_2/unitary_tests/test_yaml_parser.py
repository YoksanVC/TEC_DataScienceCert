# Unitary tests for functions in yaml_parser.py library
# General Imports
import yaml
from library.yaml_parser import yaml_to_spark_df

def test_create_spark_df(spark_session):
    """ Create a Spark dataframe form a correct YAML object"""
    yaml_string = """
- numero_caja: 10
- compras:
    - compra:
        - producto:
            -   nombre: Manzanas
                cantidad: 4
                precio_unitario: 650
        - producto:
            -   nombre: Platanos
                cantidad: 2
                precio_unitario: 1140
    - compra:
        - producto:
            -   nombre: Leche
                cantidad: 1
                precio_unitario: 890
        - producto:
            -   nombre: Pan
                cantidad: 1
                precio_unitario: 900
        - producto:
            -   nombre: Camote
                cantidad: 3
                precio_unitario: 320
    - compra:
        - producto:
            -   nombre: Queso
                cantidad: 1
                precio_unitario: 1400
"""

    yaml_data = yaml.safe_load(yaml_string)
    df_from_yaml = yaml_to_spark_df(yaml_data)

    expected_ds = spark_session.createDataFrame(
        [
            (10, 1, 1, 'Manzanas', 4, 650),
            (10, 1, 2, 'Platanos', 2, 1140),
            (10, 2, 1, 'Leche', 1, 890),
            (10, 2, 2, 'Pan', 1, 900),
            (10, 2, 3, 'Camote', 3, 320),
            (10, 3, 1, 'Queso', 1, 1400),
        ],
        ['Numero_Caja', 'Numero_Compra', 'Numero_Producto', 'Nombre', 'Cantidad', 'Precio'])

    expected_ds.show()
    df_from_yaml.show()

    assert df_from_yaml.collect() == expected_ds.collect()