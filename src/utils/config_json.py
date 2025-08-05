from rich import print
import os
from json import load
from logs.log import Log


class JsonConfig:
    """
    Fornece uma interface centralizada para ler arquivos de configuração JSON.

    Esta classe utiliza métodos de classe para carregar configurações
    essenciais para a aplicação a partir de arquivos JSON localizados no
    mesmo diretório do código-fonte.

    Atenção: Todos os métodos nesta classe irão encerrar a aplicação
    (via `SystemExit`) se o arquivo de configuração correspondente não
    puder ser encontrado ou lido.
    """

    __PATH: str = os.path.dirname(os.path.abspath(__file__))
    __FILE_PATH_TABLES: str = os.path.join(__PATH, "systems.json")
    __FILE_PATH_ORIGIN: str = os.path.join(__PATH, "origin.json")
    __FILE_PATH_DESTINY: str = os.path.join(__PATH, "destiny.json")
    __FILE_PATH_CITYS: str = os.path.join(__PATH, "citys.json")

    @classmethod
    def get_tables(cls) -> dict[str: str]:
        """
        Carrega as configurações de mapeamento de tabelas do arquivo 'systems.json'.

        Returns:
            (dict[str, str]): Um dicionário com os dados de configuração de tabelas.

        Raises:
            SystemExit: Se o arquivo 'systems.json' não for encontrado ou
                        estiver malformado.
        """
        try:
            with open(cls.__FILE_PATH_TABLES, encoding="utf-8") as file:
                data = load(file)

        except Exception as error:
            print("[bold red]Erro no arquivo de configuração, verifique o log.[/bold red]")
            Log.warning("O arquivo systems.json não pôde ser encontrado ou aberto.")
            raise SystemExit from error

        return data

    @classmethod
    def get_origin_db(cls) -> dict[str: str]:
        """
        Carrega as configurações de conexão do banco de dados de origem.

        Returns:
            (dict[str, str]): Um dicionário com os dados do arquivo 'origin.json'.

        Raises:
            SystemExit: Se o arquivo 'origin.json' não for encontrado ou
                        estiver malformado.
        """
        try:
            with open(cls.__FILE_PATH_ORIGIN, encoding="utf-8") as file:
                data = load(file)

        except Exception as error:
            print("[bold red]Erro no arquivo de configuração, verifique o log.[/bold red]")
            Log.warning("O arquivo origin.json não pôde ser encontrado ou aberto.")
            raise SystemExit from error

        return data

    @classmethod
    def get_destiny_db(cls) -> dict[str: str]:
        """
        Carrega as configurações de conexão do banco de dados de destino.

        Returns:
            (dict[str, str]): Um dicionário com os dados do arquivo 'destiny.json'.

        Raises:
            SystemExit: Se o arquivo 'destiny.json' não for encontrado ou
                        estiver malformado.
        """
        try:
            with open(cls.__FILE_PATH_DESTINY, encoding="utf-8") as file:
                data = load(file)

        except Exception as error:
            print("[bold red]Erro no arquivo de configuração, verifique o log.[/bold red]")
            Log.warning("O arquivo destiny.json não pôde ser encontrado ou aberto.")
            raise SystemExit from error

        return data

    @classmethod
    def get_citys(cls) -> dict[str: str]:
        """
        Carrega as configurações de mapeamento de cidades do arquivo 'citys.json'.

        Returns:
            (dict[str, str]): Um dicionário com os dados do arquivo 'citys.json'.

        Raises:
            SystemExit: Se o arquivo 'citys.json' não for encontrado ou
                        estiver malformado.
        """
        try:
            with open(cls.__FILE_PATH_CITYS, encoding="utf-8") as file:
                data = load(file)

        except Exception as error:
            print("[bold red]Erro no arquivo de configuração, verifique o log.[/bold red]")
            Log.warning("O arquivo citys.json não pôde ser encontrado ou aberto.")
            raise SystemExit from error

        return data
