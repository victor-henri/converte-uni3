from ..utils.connector import SQLConnector
from sqlalchemy import text


def test_connection():

    statement = text("SELECT 1;")
 
    INFO = {'host': '127.0.0.1', 
            'database': 'DADOS', 
            'user': 'sa', 
            'password': 'epilef', 
            'driver': 'driver=ODBC+Driver+17+for+SQL+Server'}
    
    engine_name = f"mssql+pyodbc://{INFO['user']}:{INFO['password']}@{INFO['host']}/{INFO['database']}?{INFO['driver']}"

    conn = SQLConnector()
    conn.db_connection(engine_name)
    engine = conn.get_engine()

    with engine.connect() as connection:
        result = connection.execute(statement)
        assert result.fetchone()[0] == 1
