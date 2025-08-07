from unicodedata import normalize
from rich import print
from pandas import DataFrame
from logs.log import Log
from utils.config_json import JsonConfig


class FieldHandler:
    """
    Fornece uma coleção de métodos estáticos para transformar colunas de DataFrames.

    Esta classe atua como uma biblioteca de funções de transformação de dados,
    projetada para ser usada dinamicamente por um pipeline de ETL.

    Atenção: Todos os métodos de transformação nesta classe irão encerrar a
    aplicação (via `SystemExit`) caso qualquer erro inesperado ocorra durante
    a sua execução.
    """

    @classmethod
    def trim(cls, df: DataFrame, column: str, **kwargs) -> DataFrame:
        """
        Remove espaços em branco do início e do fim dos valores da coluna.
        
        Args:
            df (DataFrame): DataFrame a ser modificado.
            column (str): Coluna selecionada para modificação.

        Raises:
            SystemExit: Se ocorrer um erro durante a conversão ou manipulação da string.
        """
        try:
            df[column] = df[column].astype(str).str.strip()
            return df

        except Exception as error:
            print("[bold red]Erro ao transformar dados, verifique o log.[/bold red]")
            Log.error(f"Erro ao aplicar 'trim' na coluna '{column}': {error}", True)
            raise SystemExit from error

    @classmethod
    def upper(cls, df: DataFrame, column: str, **kwargs) -> DataFrame:
        """
        Converte todos os caracteres da coluna para maiúsculas.
        
        Args:
            df (DataFrame): DataFrame a ser modificado.
            column (str): Coluna selecionada para modificação.

        Raises:
            SystemExit: Se ocorrer um erro durante a conversão ou manipulação da string.
        """
        try:
            df[column] = df[column].astype(str).str.upper()
            return df

        except Exception as error:
            print("[bold red]Erro ao transformar dados, verifique o log.[/bold red]")
            Log.error(f"Erro ao aplicar 'upper' na coluna '{column}': {error}", True)
            raise SystemExit from error

    @classmethod
    def lower(cls, df: DataFrame, column: str, **kwargs) -> DataFrame:
        """
        Converte todos os caracteres da coluna para minúsculas.
        
        Args:
            df (DataFrame): DataFrame a ser modificado.
            column (str): Coluna selecionada para modificação.

        Raises:
            SystemExit: Se ocorrer um erro durante a conversão ou manipulação da string.
        """
        try:
            df[column] = df[column].astype(str).str.lower()
            return df

        except Exception as error:
            print("[bold red]Erro ao transformar dados, verifique o log.[/bold red]")
            Log.error(f"Erro ao aplicar 'lower' na coluna '{column}': {error}", True)
            raise SystemExit from error

    @classmethod
    def switch(cls, df: DataFrame, column: str, **kwargs) -> DataFrame:
        """
        Substitui múltiplos valores na coluna com base em listas de 'de/para'.

        Args:
            df (DataFrame): DataFrame a ser modificado.
            column (str): Coluna selecionada para modificação.
            **kwargs: Espera 'option_data' com 'str_from' (valor a
                      ser substituído) e 'str_to' (novo valor).

        Raises:
            SystemExit: Se ocorrer um erro durante a substituição dos valores.
        """
        str_from = kwargs['option_data']['str_from']
        str_to = kwargs['option_data']['str_to']
        df[column] = df[column].astype(str)

        index = 0
        for value in str_from:
            try:
                df[column] = df[column].str.replace(r'\b{}\b'.format(value), str_to[index], regex=True).copy()
                index += 1

            except Exception as error:
                print("[bold red]Erro ao transformar dados, verifique o log.[/bold red]")
                Log.error(f"Erro ao aplicar 'switch' na coluna '{column}': {error}", True)
                raise SystemExit from error

        return df

    @classmethod
    def rename(cls, df: DataFrame, column: str, **kwargs) -> DataFrame:
        """
        Renomeia a coluna alvo para o valor fixo 'Codigo_Old'.
        
        Args:
            df (DataFrame): DataFrame a ser modificado.
            column (str): Coluna selecionada para modificação.

        Raises:
            SystemExit: Se a coluna alvo não existir no DataFrame.
        """
        try:
            df = df.rename(columns={column:"Codigo_Old"}).copy()
            return df

        except Exception as error:
            print("[bold red]Erro ao transformar dados, verifique o log.[/bold red]")
            Log.error(f"Erro ao aplicar 'rename' na coluna '{column}': {error}", True)
            raise SystemExit from error

    @classmethod
    def clear(cls, df: DataFrame, column: str, **kwargs) -> DataFrame:
        """
        Limpa a coluna, trocando caracteres especiais ou com 
        acentos por seus equivalentes normais.
        
        Args:
            df (DataFrame): DataFrame a ser modificado.
            column (str): Coluna selecionada para modificação.

        Raises:
            SystemExit: Se ocorrer um erro durante a limpeza ou normalização.
        """
        default = r'[^a-zA-Z0-9\s\n.-\/àáâãäèéêëìíîïòóôõöùúûüçñÀÁÂÃÄÈÉÊËÌÍÎÏÒÓÔÕÖÙÚÛÜÇÑº*ª]'

        try:
            df[column] = df[column].astype(str)
            df[column] = df[column].str.replace(default, '', regex=True).copy()
            df[column] = df[column].map(lambda x: normalize('NFKD', x).encode('ASCII', 'ignore').decode('ASCII')).copy()
            return df

        except Exception as error:
            print("[bold red]Erro ao transformar dados, verifique o log.[/bold red]")
            Log.error(f"Erro ao aplicar 'clear' na coluna '{column}': {error}", True)
            raise SystemExit from error

    @classmethod
    def select(cls, df: DataFrame, column: str, **kwargs) -> DataFrame:
        """
        Filtra o DataFrame, mantendo apenas as linhas que possuem um valor específico.

        Args:
            df (DataFrame): DataFrame a ser modificado.
            column (str): Coluna selecionada para modificação.
            **kwargs: Espera 'option_data' contendo o valor a ser usado no filtro.

        Raises:
            SystemExit: Se a coluna de filtro não existir ou ocorrer outro erro.
        """
        filter = kwargs['option_data']
        try:
            df = df[df[column] == filter].reset_index(drop=True).copy()
            return df

        except Exception as error:
            print("[bold red]Erro ao transformar dados, verifique o log.[/bold red]")
            Log.error(f"Erro ao aplicar 'select' na coluna '{column}': {error}", True)
            raise SystemExit from error

    @classmethod
    def copy(cls, df: DataFrame, column: str, **kwargs) -> DataFrame:
        """
        Copia os valores de uma outra coluna para a coluna alvo.

        Args:
            df (DataFrame): DataFrame a ser modificado.
            column (str): Coluna selecionada para modificação.
            **kwargs: Espera 'option_data' com o nome da coluna de origem.

        Raises:
            SystemExit: Se a coluna de origem não existir no DataFrame.
        """
        option_data = kwargs['option_data']
        try:
            df[column] = df[option_data]
            return df

        except Exception as error:
            print("[bold red]Erro ao transformar dados, verifique o log.[/bold red]")
            Log.error(f"Erro ao aplicar 'copy' na coluna '{column}': {error}", True)
            raise SystemExit from error

    @classmethod
    def format(cls, df: DataFrame, column: str, **kwargs) -> DataFrame:
        """
        Formata os valores da coluna para padrões específicos (CPF, CNPJ, etc.).

        Args:
            df (DataFrame): DataFrame a ser modificado.
            column (str): Coluna selecionada para modificação.
            **kwargs: Espera 'option_data' com o tipo de formato desejado,
                      ex: 'CPF', 'CNPJ', 'DATETIME', 'CEP'.
        
        Raises:
            SystemExit: Se ocorrer um erro durante a aplicação do formato.
        """
        str_type = kwargs['option_data']
        df[column] = df[column].astype(str)

        if str_type == 'CPF':
            search = r'([0-9]{3})([0-9]{3})([0-9]{3})([0-9]{2})'
            format = r'\1.\2.\3-\4'
            try:
                df[column] = df[column].str.replace(search, format, regex=True).copy()
                return df

            except Exception as error:
                print("[bold red]Erro ao transformar dados, verifique o log.[/bold red]")
                Log.error(f"Erro ao aplicar 'format' na coluna '{column}': {error}", True)
                raise SystemExit from error

        elif str_type == 'CNPJ':
            search = r'([0-9]{2})([0-9]{3})([0-9]{3})([0-9]{4})([0-9]{2})'
            format = r'\1.\2.\3/\4-\5'
            try:
                df[column] = df[column].str.replace(search, format, regex=True).copy()
                return df

            except Exception as error:
                print("[bold red]Erro ao transformar dados, verifique o log.[/bold red]")
                Log.error(f"Erro ao aplicar 'format' na coluna '{column}': {error}", True)
                raise SystemExit from error

        elif str_type == 'DATETIME':
            search = r'([0-9]{4})[-./ ]?([0-9]{2})[-./ ]?([0-9]{2})'
            format = r'\1-\2-\3 00:00:00.000'
            try:
                df[column] = df[column].str.replace(search, format, regex=True).copy()
                return df

            except Exception as error:
                print("[bold red]Erro ao transformar dados, verifique o log.[/bold red]")
                Log.error(f"Erro ao aplicar 'format' na coluna '{column}': {error}", True)
                raise SystemExit from error

        elif str_type == 'CEP':
            search = r'([0-9]{5})([0-9]{3})'
            format = r'\1-\2'

            try:
                df[column] = df[column].str.replace(search, format, regex=True).copy()
                return df

            except Exception as error:
                print("[bold red]Erro ao transformar dados, verifique o log.[/bold red]")
                Log.error(f"Erro ao aplicar 'format' na coluna '{column}': {error}", True)
                raise SystemExit from error

    @classmethod
    def split(cls, df: DataFrame, column: str, **kwargs) -> DataFrame:
        """
        Divide uma coluna de telefone em novas colunas de DDD e número.

        Args:
            df (DataFrame): DataFrame a ser modificado.
            column (str): Coluna selecionada para modificação.
            **kwargs: Espera 'option_data' indicando o tipo de campo a ser
                      dividido.

        Raises:
            SystemExit: Se ocorrer um erro durante a extração com regex.
        """
        split_field = kwargs['option_data']
        df[column] = df[column].astype(str)

        if split_field == 'DDD1':
            try:
                splited = df[column].str.extract(r'^(?P<DDD1>[0-9]{2})?(?P<Fone_Numero>[0-9]{8,9})').copy()
                df = df.assign(DDD1=splited[split_field],Fone_Numero=splited[column]).copy()
                return df

            except Exception as error:
                print("[bold red]Erro ao transformar dados, verifique o log.[/bold red]")
                Log.error(f"Erro ao aplicar 'split' na coluna '{column}': {error}", True)
                raise SystemExit from error

        else:
            try:
                splited = df[column].str.extract(r'^(?P<DDD_Celular>[0-9]{2})?(?P<Numero_Celular>[0-9]{8,9})').copy()
                df = df.assign(DDD_Celular=splited[split_field],Numero_Celular=splited[column]).copy()
                return df

            except Exception as error:
                print("[bold red]Erro ao transformar dados, verifique o log.[/bold red]")
                Log.error(f"Erro ao aplicar 'split' na coluna '{column}': {error}", True)
                raise SystemExit from error

    @classmethod
    def search(cls, df: DataFrame, column: str, **kwargs) -> DataFrame:
        """
        Enriquece o DataFrame buscando dados de cidades em uma fonte externa.

        Args:
            df (DataFrame): DataFrame a ser modificado.
            column (str): Coluna selecionada para modificação.
            **kwargs: Espera 'option_data' com o tipo de busca, 
                      ex: 'CITY'.

        Raises:
            SystemExit: Se ocorrer um erro durante a busca ou junção dos dados.
        """
        search_type = kwargs['option_data']

        if search_type == 'CITY':
            citys = JsonConfig.get_citys()
            df_city = DataFrame(citys)

            try:
                df_city = df_city.rename(columns={'Codigo':'Codigo_Cidade'}).copy()
                df_city = df_city.set_index(['Cidade', 'Estado'])
                df = df.join(df_city, on=[column, 'UF']).copy()
                return df

            except Exception as error:
                print("[bold red]Erro ao transformar dados, verifique o log.[/bold red]")
                Log.error(f"Erro ao aplicar 'search' na coluna '{column}': {error}", True)
                raise SystemExit from error
