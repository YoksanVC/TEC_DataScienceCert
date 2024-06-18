# General Librarias
from library.data_transformation import bpm_correction


def test_low_bpm_default(spark_session):
    """ Test low bpm correction - Using default of 80 bpm"""
    sport_data = [(1, 68, 1000), (2, 145, 2500), (3, 198, 3300), (4, 80, 1200)]
    #sport_data = [(1, 80, 1000), (2, 145, 2500), (3, 198, 3300), (4, 81, 1200)] # Fail Case to try
    sport_ds = spark_session.createDataFrame(sport_data,['ID', 'Ritmo Cardiaco', 'Distancia Total (m)'])
    sport_ds.show()

    corrected_ds = bpm_correction(sport_ds)

    expected_ds = spark_session.createDataFrame(
        [
            (1, 120, 1000),
            (2, 145, 2500),
            (3, 198, 3300),
            (4, 80, 1200),
        ],
        ['ID', 'Ritmo Cardiaco', 'Distancia Total (m)'])

    expected_ds.show()
    corrected_ds.show()

    assert corrected_ds.collect() == expected_ds.collect()

def test_low_bpm_newLimit(spark_session):
    """ Test low bpm correction - Using a new limit of 100 bpm"""
    sport_data = [(1, 101, 1000), (2, 98, 2500), (3, 198, 3300), (4, None, 1200)]
    #sport_data = [(1, 99, 1000), (2, 98, 2500), (3, 98, 3300), (4, 76, 1200)] # Fail Case to try
    sport_ds = spark_session.createDataFrame(sport_data,['ID', 'Ritmo Cardiaco', 'Distancia Total (m)'])
    sport_ds.show()

    corrected_ds = bpm_correction(sport_ds,100)

    expected_ds = spark_session.createDataFrame(
        [
            (1, 101, 1000),
            (2, 120, 2500),
            (3, 198, 3300),
            (4, 120, 1200),
        ],
        ['ID', 'Ritmo Cardiaco', 'Distancia Total (m)'])

    expected_ds.show()
    corrected_ds.show()

    assert corrected_ds.collect() == expected_ds.collect()
