from datetime import date
from rich import print
from pandas import DataFrame
from sqlalchemy import MetaData, Table, Engine
from utils.log import Log
from stages.contracts.extract_contract import ExtractContract
from stages.interfaces.sql_extractor import ExtractInterface


class Extractor(ExtractInterface):
    """
    Implementa o processo de extração para bancos de dados relacionais via SQLAlchemy.
    
    Esta classe se conecta a um banco de dados, executa a extração de
    múltiplas tabelas e retorna os dados em um objeto de contrato padronizado.

    Args:
            font (str): Um identificador para a fonte de dados.
            engine (Engine): Uma instância ativa do engine do SQLAlchemy para a 
                             conexão com o banco de dados.
    """

    def __init__(self,  font: str, engine: Engine) -> None:
        self.__metadata: MetaData = MetaData()
        self.__engine = engine
        self.__font = font
        self.__dfs: dict[str, DataFrame] = {}

    def extract(self, tables: dict[str, str]) -> ExtractContract:
        """
        Orquestra a extração de dados de múltiplas tabelas do banco de dados.

        Este método itera sobre o dicionário de tabelas fornecido, chama o
        método auxiliar `__extract_table` para cada uma e agrega os DataFrames
        resultantes antes de empacotá-los em um ExtractContract.

        Args:
            tables (dict[str, dict]): Dicionário que mapeia um nome lógico 
                                      para as especificações da tabela.

        Returns:
            ExtractContract: Um objeto de contrato contendo um dicionário de
                             DataFrames com os dados brutos e a data da extração.
        """
        for name, table in tables.items():
            data = self.__extract_table(table["table"])
            self.__dfs.update({name: data})

        return ExtractContract(
            font=self.__font,
            raw_data=self.__dfs,
            extraction_date=date.today()
        )

    def __extract_table(self, table_name: str) -> DataFrame:
        """
        Busca todos os registros de uma única tabela do banco de dados.

        Utiliza a funcionalidade de reflexão do SQLAlchemy (`autoload_with`) para
        carregar a estrutura da tabela e, em seguida, executa uma consulta
        para selecionar todos os seus dados.

        Args:
            table_name (str): O nome exato da tabela no banco de dados.

        Returns:
            DataFrame: Um DataFrame do pandas com os dados da tabela.

        Raises:
            SystemExit: Em caso de qualquer erro durante a conexão ou consulta,
                        o erro é logado e a aplicação é encerrada.
        """
        try:
            table = Table(table_name, self.__metadata, autoload_with=self.__engine)

            with self.__engine.connect() as connection:
                result = connection.execute(table.select())
                rows = result.fetchall()

            return DataFrame(rows, columns=result.keys())

        except Exception as error:
            print("[bold red]Erro ao extrair dados, verifique o log.[/bold red]")
            Log.error(f"Erro ao extrair dados da TABELA: {table_name}: ERRO: {error}", True)
            raise SystemExit from error
