from rich import print
from pandas import DataFrame
from sqlalchemy import Engine
from logs.log import Log
from stages.contracts.transform_contract import TransformContract
from stages.interfaces.load_data import LoadInterface


class Loader(LoadInterface):
    """
    Implementa a fase de Carga (Load) do pipeline ETL para bancos de dados.

    Esta classe recebe dados limpos de um contrato de transformação e utiliza
    SQLAlchemy e pandas para inserir os registros nas tabelas de destino.

    Args:
            str: Um identificador para a fonte original dos dados.
            Engine: Uma instância ativa do engine do SQLAlchemy para a
                             conexão com o banco de dados de destino.
            TransformContract: O objeto de contrato que contém
                                                   os DataFrames limpos.
    """

    def __init__(self, transform_contract: TransformContract, engine: Engine):
        self.__clean_data: dict[str, DataFrame] = transform_contract.clean_data
        self.__engine = engine

    def load(self) -> None:
        """Inicia o processo de carga dos dados limpos no banco de dados."""
        self.__insert_table()

    def __insert_table(self) -> None:
        """
        Itera e insere cada DataFrame na tabela de banco de dados correspondente.

        Utiliza o método `pandas.to_sql` com a estratégia 'append' para adicionar
        os novos registros às tabelas existentes.

        Raises:
            SystemExit: Em caso de qualquer erro durante a inserção,
                        o erro é logado e a aplicação é encerrada.
        """
        for table, df in self.__clean_data.items():
            try:
                df.to_sql(name=table, con=self.__engine, if_exists='append', index=False)

            except Exception as error:
                print("[bold red]Erro ao carregar dados, verifique o log.[/bold red]")
                Log.error(f"Erro ao inserir dados na tabela {table}: {error}", True)
                raise SystemExit from error
