from library.data_transformation import lower_case

def test_lower_case_string(spark_session):
    """ Test lowering the case for stringtype columns """
    jugadores_data = [('Leonel Messi', 220, 'InteR MiAmi'),
                ('andres Iniesta', 129, 'barcelona'),
                ('Diego maradona', 198, 'nApOlI')]
    jugadores_ds = spark_session.createDataFrame(jugadores_data,['Jugador', 'Goles', 'Equipo'])

    lower_case_ds = lower_case(jugadores_ds)

    expected_ds = spark_session.createDataFrame(
        [
            ('leonel messi', 220, 'inter miami'),
            ('andres iniesta', 129, 'barcelona'),
            ('diego maradona', 198, 'napoli'),
        ],
        ['Jugador', 'Goles', 'Equipo'])

    expected_ds.show()
    lower_case_ds.show()

    assert lower_case_ds.collect() == expected_ds.collect()