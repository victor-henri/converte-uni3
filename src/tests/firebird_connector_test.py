from ..utils.connector import SQLConnector
from sqlalchemy import text


def test_connection():

    statement = text("SELECT 1 FROM RDB$DATABASE;")
 
    INFO = {'font': 'Firebird',
            'host': 'localhost',
            'user': 'sysdba', 
            'password': 'masterkey',
            'database': 'C:/Firebird/Matriz_Olimpus/MATRIZ.FDB'}
    
    engine_name = f"firebird+fdb://{INFO['user']}:{INFO['password']}@{INFO['host']}/{INFO['database']}"

    conn = SQLConnector()
    conn.db_connection(engine_name)
    engine = conn.get_engine()

    with engine.connect() as connection:
        result = connection.execute(statement)
        assert result.fetchone()[0] == 1
