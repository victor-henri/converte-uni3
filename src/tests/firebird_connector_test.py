from src.utils.connector import SQLConnector
from sqlalchemy import text


def test_connection():

    statement = text("SELECT 1 FROM RDB$DATABASE;")

    INFO = {"font": "Firebird",
            "host": "localhost",
            "user": "sysdba", 
            "password": "masterkey",
            "database": "C:/Moura_/Banco_Origem/OLIMPUS.FDB"}

    conn = SQLConnector()
    conn.db_connection(INFO)
    engine = conn.get_engine()

    with engine.connect() as connection:
        result = connection.execute(statement)
        assert result.fetchone()[0] == 1
