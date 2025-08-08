# Pipeline ETL - Sistema de Extract, Transform, Load
# Copyright (C) 2025 Victor Henrique Gonçalves dos Santos
#
# Este programa é um software livre; você pode redistribuí-lo e/ou
# modificá-lo sob os termos da Licença Pública Geral GNU como
# publicada pela Free Software Foundation; na versão 3 da Licença.
#
# Este programa é distribuído na esperança de que seja útil,
# mas SEM NENHUMA GARANTIA; sem mesmo a garantia implícita de
# COMERCIALIZAÇÃO ou ADEQUAÇÃO A UM DETERMINADO FIM. Consulte a
# Licença Pública Geral GNU para mais detalhes.
#
# Você deve ter recebido uma cópia da Licença Pública Geral GNU junto
# com este programa. Se não, veja <https://www.gnu.org/licenses/>.

from rich import print
from pandas import DataFrame
from sqlalchemy import Engine
from utils.log import Log
from stages.contracts.transform_contract import TransformContract
from stages.interfaces.load_data import LoadInterface


class Loader(LoadInterface):
    """
    Implementa a fase de Carga (Load) do pipeline ETL para bancos de dados.

    Esta classe recebe dados limpos de um contrato de transformação e utiliza
    SQLAlchemy e pandas para inserir os registros nas tabelas de destino.

    Args:
            transform_contract (TransformContract): O objeto de contrato que contém
                                                    os DataFrames limpos.
            engine (Engine): Uma instância ativa do engine do SQLAlchemy para a
                             conexão com o banco de dados de destino.
            
    """

    def __init__(self, transform_contract: TransformContract, engine: Engine):
        self.__clean_data: list[dict[str, DataFrame]] = transform_contract.clean_data
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
        for item in self.__clean_data:
            for table, df in item.items():
                try:
                    df.to_sql(name=table, con=self.__engine, if_exists='append', index=False)

                except Exception as error:
                    print("[bold red]Erro ao carregar dados, verifique o log.[/bold red]")
                    Log.error(f"Erro ao inserir dados na tabela {table}: {error}", True)
                    raise SystemExit from error
