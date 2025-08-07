import pandas as pd
from src.stages.transform.field_utils import FieldHandler


def test_trim_method():

    test_data = {
        'name': ['  John  ', 'Jane  ', '  Bob', 'Alice   ']
    }
    df = pd.DataFrame(test_data)
    expected_data = {
        'name': ['John', 'Jane', 'Bob', 'Alice']
    }
    expected_df = pd.DataFrame(expected_data)

    result_df = FieldHandler.trim(df, 'name')

    pd.testing.assert_frame_equal(result_df, expected_df)

def test_upper_method():

    test_data = {
        'text': ['hello', 'World', 'Python', 'test']
    }
    df = pd.DataFrame(test_data)
    expected_data = {
        'text': ['HELLO', 'WORLD', 'PYTHON', 'TEST']
    }
    expected_df = pd.DataFrame(expected_data)

    result_df = FieldHandler.upper(df, 'text')

    pd.testing.assert_frame_equal(result_df, expected_df)

def test_lower_method():

    test_data = {
        'text': ['HELLO', 'WORLD', 'PYTHON', 'TEST']
    }
    df = pd.DataFrame(test_data)
    expected_data = {
        'text': ['hello', 'world', 'python', 'test']
    }
    expected_df = pd.DataFrame(expected_data)

    result_df = FieldHandler.lower(df, 'text')

    pd.testing.assert_frame_equal(result_df, expected_df)

def test_switch_method():

    test_data = {
        'status': ['active', 'inactive', 'active', 'pending']
    }
    df = pd.DataFrame(test_data)
    expected_data = {
        'status': ['enabled', 'inactive', 'enabled', 'pending']
    }
    expected_df = pd.DataFrame(expected_data)

    result_df = FieldHandler.switch(df, 'status', option_data = {'str_from':['active'], 'str_to':['enabled']})

    pd.testing.assert_frame_equal(result_df, expected_df)

def test_rename_method():

    test_data = {
        'id': [1, 2, 3, 4]
    }
    df = pd.DataFrame(test_data)

    result_df = FieldHandler.rename(df, 'id')

    assert 'Codigo_Old' in result_df.columns
    assert 'id' not in result_df.columns

def test_clear_method():

    test_data = {
        'text': ['áéíóú', 'çñ', '@#$%', 'test!']
    }
    df = pd.DataFrame(test_data)
    expected_data = {
        'text': ['aeiou', 'cn', '', 'test']
    }
    expected_df = pd.DataFrame(expected_data)

    result_df = FieldHandler.clear(df, 'text')

    pd.testing.assert_frame_equal(result_df, expected_df)

def test_select_method():

    test_data = {
        'status': ['active', 'inactive', 'active', 'pending']
    }
    df = pd.DataFrame(test_data)
    expected_data = {
        'status': ['active', 'active']
    }
    expected_df = pd.DataFrame(expected_data)

    result_df = FieldHandler.select(df, 'status', option_data='active')

    pd.testing.assert_frame_equal(result_df, expected_df)

def test_format_cpf():

    test_data = {
        'CPF': ['12345678901', '98765432109']
    }
    df = pd.DataFrame(test_data)
    expected_data = {
        'CPF': ['123.456.789-01', '987.654.321-09']
    }
    expected_df = pd.DataFrame(expected_data)

    result_df = FieldHandler.format(df, 'CPF', option_data='CPF')

    pd.testing.assert_frame_equal(result_df, expected_df)

def test_format_cnpj():

    test_data = {
        'CNPJ': ['12345678901234', '98765432109876']
    }
    df = pd.DataFrame(test_data)
    expected_data = {
        'CNPJ': ['12.345.678/9012-34', '98.765.432/1098-76']
    }
    expected_df = pd.DataFrame(expected_data)

    result_df = FieldHandler.format(df, 'CNPJ', option_data='CNPJ')

    pd.testing.assert_frame_equal(result_df, expected_df)

def test_format_datetime():

    test_data = {
        'date': ['2023-12-25', '2024-01-01']
    }
    df = pd.DataFrame(test_data)
    expected_data = {
        'date': ['2023-12-25 00:00:00.000', '2024-01-01 00:00:00.000']
    }
    expected_df = pd.DataFrame(expected_data)

    result_df = FieldHandler.format(df, 'date', option_data='DATETIME')

    pd.testing.assert_frame_equal(result_df, expected_df)

def test_format_cep():

    test_data = {
        'cep': ['12345678', '87654321']
    }
    df = pd.DataFrame(test_data)
    expected_data = {
        'cep': ['12345-678', '87654-321']
    }
    expected_df = pd.DataFrame(expected_data)

    result_df = FieldHandler.format(df, 'cep', option_data='CEP')

    pd.testing.assert_frame_equal(result_df, expected_df)

def test_split_ddd1():

    test_data = {
        'Fone_Numero': ['11999999999', '21888888888']
    }
    df = pd.DataFrame(test_data)

    result_df = FieldHandler.split(df, 'Fone_Numero', option_data='DDD1')

    assert 'DDD1' in result_df.columns
    assert result_df['DDD1'].tolist() == ['11', '21']

def test_split_celular():

    test_data = {
        'Numero_Celular': ['11999999999', '21888888888']
    }
    df = pd.DataFrame(test_data)

    result_df = FieldHandler.split(df, 'Numero_Celular', option_data='DDD_Celular')

    assert 'DDD_Celular' in result_df.columns
    assert result_df['DDD_Celular'].tolist() == ['11', '21']

def test_search_method():

    test_data = {
        'city': ['BELO HORIZONTE', 'RIO DE JANEIRO'],
        'UF': ['MG', 'RJ']
    }
    df = pd.DataFrame(test_data)

    result_df = FieldHandler.search(df, 'city', option_data='CITY')

    assert 'Codigo_Cidade' in result_df.columns
    assert result_df['Codigo_Cidade'].tolist() == [2547, 6343]
