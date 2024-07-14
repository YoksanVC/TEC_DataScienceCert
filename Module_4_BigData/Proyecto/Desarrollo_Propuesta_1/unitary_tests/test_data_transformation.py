from library.data_transformation import lower_case, dataframe_joiner_title

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

def test_dataframe_joiner_title(spark_session):
    """ Test the join function between the values in title columns """
    df1_data = [('Pasion de Gavilanes', 'Netflix'),
                ('Betty la fea', 'Max'),
                ('La Usurpadora', 'Amazon Prime')]
    df1_ds = spark_session.createDataFrame(df1_data,['game_title', 'Streaming Service'])
    df1_ds.show()

    # datafram2 with an extra row that will not be take into consideration, then
    df2_data = [('Marimar', 0.2),
                ('Betty la fea', 3.6),
                ('Pasion de Gavilanes', 2.7),
                ('La Usurpadora', 0.9)]
    df2_ds = spark_session.createDataFrame(df2_data,['Title', 'Avg Audience (Millions)'])
    df2_ds.show()

    joint_ds = dataframe_joiner_title(df1_ds,df2_ds)

    expected_ds = spark_session.createDataFrame(
        [
            ('Betty la fea', 'Max', 'Betty la fea', 3.6),
            ('La Usurpadora', 'Amazon Prime', 'La Usurpadora', 0.9),
            ('Pasion de Gavilanes', 'Netflix', 'Pasion de Gavilanes', 2.7),
        ],
        ['game_title', 'Streaming Service', 'Title', 'Avg Audience (Millions)'])

    expected_ds.show()
    joint_ds.show()

    assert joint_ds.collect() == expected_ds.collect()

def test_dataframe_joiner_title_missing_column(spark_session):
    """ Test failure mode for not having the correct columns name """
    df1_data = [('Pasion de Gavilanes', 'Netflix'),
                ('Betty la fea', 'Max'),
                ('La Usurpadora', 'Amazon Prime')]
    df1_ds = spark_session.createDataFrame(df1_data,['game_title', 'Streaming Service'])
    df1_ds.show()

    # datafram2 with an extra row that will not be take into consideration, then
    df2_data = [('Marimar', 0.2),
                ('Betty la fea', 3.6),
                ('Pasion de Gavilanes', 2.7),
                ('La Usurpadora', 0.9)]
    df2_ds = spark_session.createDataFrame(df2_data,['titulo', 'Avg Audience (Millions)'])
    df2_ds.show()

    joint_ds = dataframe_joiner_title(df1_ds,df2_ds)


    assert joint_ds == False