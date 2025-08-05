from src.logs.log import Log

def test_log():
    
    Log.warning('Teste de Warning')
    Log.info('Teste de Info')
    Log.error('Teste de Erro', True)
    Log.critical('Teste Critico', True)