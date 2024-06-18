# General Libraries
from library.data_analysis import top_athletes_totalDistance_perCountry

def test_top_athletes_total_distance_oneCountry(spark_session):
    """ Test to create dataframes pre country to show the top athletes by total distance covered """
    sports_data = [('Marco', 'Costa Rica', 20), ('Marco', 'Costa Rica', 20), ('Jesus', 'Costa Rica', 12), ('Jesus', 'Costa Rica', 56)]
    sports_ds = spark_session.createDataFrame(sports_data,['Nombre', 'Pais', 'Distancia_Total_(m)'])
    sports_ds.show()

    joint_ds = top_athletes_totalDistance_perCountry(sports_ds)

    expected_ds = spark_session.createDataFrame(
        [
            ('Jesus', 'Costa Rica', 68),
            ('Marco', 'Costa Rica', 40),
        ],
        ['Nombre', 'Pais', 'sum(Distancia_Total_(m))'])

    expected_ds.show()
    joint_ds.show()

    assert joint_ds.collect() == expected_ds.collect()
    
def test_top_athletes_total_distance_moreCountries(spark_session):
    """ Test to create dataframes pre country to show the top athletes by total distance covered """
    sports_data = [('Marco', 'Costa Rica', 20), 
                   ('Marco', 'Costa Rica', 20), 
                   ('Jesus', 'Costa Rica', 12), 
                   ('Jesus', 'Costa Rica', 56),
                   ('Luis', 'Honduras', 54), 
                   ('Luis', 'Honduras', 22), 
                   ('Pedro', 'Honduras', 45),
                   ('Pedro', 'Honduras', 21)]
    sports_ds = spark_session.createDataFrame(sports_data,['Nombre', 'Pais', 'Distancia_Total_(m)'])
    sports_ds.show()

    joint_ds = top_athletes_totalDistance_perCountry(sports_ds)

    expected_ds = spark_session.createDataFrame(
        [
            ('Luis', 'Honduras', 76),
            ('Jesus', 'Costa Rica', 68),
            ('Marco', 'Costa Rica', 40),
            ('Pedro', 'Honduras', 66),
        ],
        ['Nombre', 'Pais', 'sum(Distancia_Total_(m))'])

    expected_ds.show()
    joint_ds.show()

    assert joint_ds.collect() == expected_ds.collect()
    
def test_top_athletes_total_distance_null(spark_session):
    """ Test failure mode when data to be sum is null """
    sports_data = [('Marco', 'Costa Rica', 20), ('Marco', 'Costa Rica', None), ('Jesus', 'Costa Rica', 12), ('Jesus', 'Costa Rica', 56)]
    sports_ds = spark_session.createDataFrame(sports_data,['Nombre', 'Pais', 'Distancia_Total_(m)'])
    sports_ds.show()

    joint_ds = top_athletes_totalDistance_perCountry(sports_ds)

    expected_ds = spark_session.createDataFrame(
        [
            ('Jesus', 'Costa Rica', 68),
            ('Marco', 'Costa Rica', 20),
        ],
        ['Nombre', 'Pais', 'sum(Distancia_Total_(m))'])

    expected_ds.show()
    joint_ds.show()

    assert joint_ds.collect() == expected_ds.collect()

def test_top_athletes_group_null(spark_session):
    """ Test failure mode when data to use for groupin is null """
    sports_data = [('Marco', 'Costa Rica', 20), ('Marco', None, 20), ('Jesus', 'Costa Rica', 12), ('Jesus', 'Costa Rica', 56)]
    sports_ds = spark_session.createDataFrame(sports_data,['Nombre', 'Pais', 'Distancia_Total_(m)'])
    sports_ds.show()

    joint_ds = top_athletes_totalDistance_perCountry(sports_ds)

    expected_ds = spark_session.createDataFrame(
        [
            ('Jesus', 'Costa Rica', 68),
            ('Marco', 'Costa Rica', 20),
        ],
        ['Nombre', 'Pais', 'sum(Distancia_Total_(m))'])

    expected_ds.show()
    joint_ds.show()

    assert joint_ds.collect() == expected_ds.collect()
    
def test_top_athletes_missingColumn(spark_session):
    """ Test failure mode when one column is missing """
    sports_data = [('Marco', 20), ('Marco', 20), ('Jesus', 12), ('Jesus', 56)]
    sports_ds = spark_session.createDataFrame(sports_data,['Nombre', 'Distancia_Total_(m)'])
    sports_ds.show()

    joint_ds = top_athletes_totalDistance_perCountry(sports_ds)

    assert joint_ds == False