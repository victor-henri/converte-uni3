from sqlalchemy import text
from src.utils.connector import SQLConnector


def test_connection():

    statement = text("SELECT 1;")

    INFO = {"font": "SQLServer",
            "host": "localhost",
            "user": "sa", 
            "password": "epilef",
            "database": "DESTINO",
            "driver": "driver=ODBC+Driver+17+for+SQL+Server"}

    conn = SQLConnector()
    conn.db_connection(INFO)
    engine = conn.get_engine()

    with engine.connect() as connection:
        result = connection.execute(statement)
        assert result.fetchone()[0] == 1
