from src.stages.extract.sql_extractor import Extractor
from src.utils.connector import SQLConnector


INFO = {'font': 'Firebird',
        'host': 'localhost', 
        'database': 'C:/Firebird/Matriz_Olimpus/MATRIZ.FDB', 
        'user': 'sysdba', 
        'password': 'masterkey'}

engine_name = f"firebird+fdb://{INFO['user']}:{INFO['password']}@{INFO['host']}/{INFO['database']}"

TABLES = {'supplier': 'CLIENTES'}


def test_extract_branches():

    conn = SQLConnector()
    conn.db_connection(engine_name)
    engine = conn.get_connection()

    extract = Extractor(INFO['font'], engine)
    data = extract.extract(TABLES)
    
    for name, table in data.raw_data.items():
        print('=============')
        print(name)
        print(table.head())