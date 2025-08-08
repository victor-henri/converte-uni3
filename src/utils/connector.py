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
from sqlalchemy import create_engine, Engine
from utils.log import Log


class SQLConnector():
    """Gerencia a criação e o armazenamento de um engine de conexão do SQLAlchemy."""

    def __init__(self) -> None:
        self.__engine: Engine = None

    def db_connection(self, info: dict[str, str]) -> None:
        """
        Cria, testa e armazena um engine de banco de dados a partir de uma string de conexão.

        Este método tenta criar um engine do SQLAlchemy e, em seguida, realiza
        uma tentativa de conexão para validar a string e as credenciais.
        Se a conexão for bem-sucedida, o engine é armazenado internamente.

        Args:
            info (dict[str, str]): Um dicionário contendo as credenciais e o tipo ('font')
                                   do banco de dados.

        Raises:
            SystemExit: Se a criação do engine ou o teste de conexão falhar por
                        qualquer motivo.
        """
        try:
            engine_name = self.__get_engine_name(info)
            engine = create_engine(engine_name)
            connection = engine.connect()
            connection.close()
            self.__set_engine(engine)

        except Exception as error:
            print("[bold red]Erro ao conectar no banco de dados, verifique o log.[/bold red]")
            Log.error(f"Erro ao conectar no banco de dados: {error}", False)
            raise SystemExit from error

    def __set_engine(self, engine: Engine) -> None:
        """
        Define o engine do SQLAlchemy para a instância do conector.

        Args:
            engine (Engine): Uma instância de engine do SQLAlchemy.
        """
        self.__engine = engine

    def get_engine(self) -> Engine:
        """
        Recupera o engine do SQLAlchemy armazenado.

        Returns:
            (Engine | None): A instância do engine do SQLAlchemy se a conexão
                             foi estabelecida com sucesso, ou None caso contrário.
        """
        return self.__engine

    def __get_engine_name(self, info: dict[str, str]) -> str:
        """
        Constrói a string de conexão do SQLAlchemy com base na configuração.

        Args:
            info (dict[str, str]): Um dicionário contendo as credenciais e o tipo ('font')
                                   do banco de dados.

        Returns:
            str: A string de conexão formatada para o SQLAlchemy.

        Raises:
            SystemExit: Se o tipo de banco de dados ('font') não for 'Firebird'
                        ou 'SQLServer', pois não é suportado.
        """
        if info['font'] == 'Firebird':
            return f"firebird+fdb://{info['user']}:{info['password']}@{info['host']}/{info['database']}"

        elif info['font'] == 'SQLServer':
            return f"mssql+pyodbc://{info['user']}:{info['password']}@{info['host']}/{info['database']}?{info['driver']}"

        else:
            print("[bold red]Base informada é invalida ou ainda não foi implementada.[/bold red]")
            Log.warning("A base de dados informada é invalida ou ainda não foi implementada.")
            raise SystemExit
