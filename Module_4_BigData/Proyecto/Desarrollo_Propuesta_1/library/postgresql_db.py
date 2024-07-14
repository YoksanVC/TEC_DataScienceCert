def save_in_db(dataframe, db_name):
    """ Function to save in a POSTGRESQL database

    Args:
        dataframe (Spark Dataframe): Dataframe to be store in DB
        db_name (String): DB name

    Returns:
        True,False: Boolean state for saving condition
    """
    try:
        dataframe \
            .write \
            .format("jdbc") \
            .mode('overwrite') \
            .option("url", "jdbc:postgresql://host.docker.internal:5433/postgres") \
            .option("user", "postgres") \
            .option("password", "testPassword") \
            .option("dbtable", db_name) \
            .option("driver", "org.postgresql.Driver") \
            .save()
        return True
    except Exception as ex:
        print(f"Error during DB writing: {ex}")
        return False
    
def read_from_db(db_name, spark_session):
    """ Function to read from a POSTGRESQL database

    Args:
        db_name (string): DB name to be read
        spark_session (Spart Session): Spark Session

    Returns:
        df: Spark Dataframe from DB read
    """
    try:
        df = spark_session \
            .read \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://host.docker.internal:5433/postgres") \
            .option("user", "postgres") \
            .option("password", "testPassword") \
            .option("dbtable", db_name) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        return df
    except Exception as ex:
        print(f"Error during DB loading: {ex}")
        return False