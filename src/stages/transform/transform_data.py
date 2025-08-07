from rich import print
from datetime import date
from pandas import DataFrame
from logs.log import Log
from stages.contracts.extract_contract import ExtractContract
from stages.contracts.transform_contract import TransformContract
from stages.interfaces.transform_data import TransformInterface
from stages.transform.field_utils import FieldHandler


class Transformer(TransformInterface):
    """
    Implementa a fase de transformação do pipeline ETL.

    Esta classe recebe dados brutos de um contrato de extração e aplica uma
    série de passos configuráveis — como seleção, renomeação e remoção
    de colunas, além de transformações de valores — para gerar dados limpos.
    A lógica de transformação de campos é delegada dinamicamente à classe
    `FieldHandler`.

    Args:
            ExtractContract: O objeto de contrato que contém
                os DataFrames brutos da fase de extração.
    """

    def __init__(self, extract_contract: ExtractContract) -> None:
        self.__raw_data: dict[str, DataFrame] = extract_contract.raw_data
        self.__processed_data: list[dict[str, DataFrame]] = []

    def transform(self, tables: dict[str, str]) -> TransformContract:
        """
        Orquestra o processo de transformação para todos os dados.

        Args:
            (dict[str, str]): A configuração que detalha os passos de
                transformação para cada tabela de dados brutos.

        Returns:
            TransformContract: Um contrato contendo os DataFrames processados.
        """
        self.__transform_tables(tables)

        return TransformContract(
            clean_data=self.__processed_data,
            transform_date=date.today()
        )

    def __transform_tables(self, tables: dict[str, str]) -> None:
        """
        Aplica o fluxo de transformação para cada DataFrame.

        Para cada item no dicionário de configuração, este método executa a
        sequência de limpeza: extrai colunas, renomeia, aplica transformações
        e remove colunas indesejadas.
        Adicionalmente cria arquivos xlsx para cada tabela transformada.

        Args:
            (dict[str, str]): A configuração detalhada das transformações.
        """
        for key, value in tables.items():
            df = self.__raw_data[key]
            info = value['fields']
            remove = value['remove']

            result = self.__extract_colunms(df, info)
            result = self.__rename(result, info)
            result = self.__transform_columns(result, info)
            result = self.__remove_columns(result, remove)

            result.to_excel(f'{key}.xlsx', index=False)
            self.__set_table(value['destiny'], result)

    def __extract_colunms(self, df: DataFrame, info: dict[str, str]) -> DataFrame:
        """
        Seleciona um subconjunto de colunas de um DataFrame.

        Args:
            DataFrame: O DataFrame original.
            (dict[str, str]): A parte da configuração que contém os nomes
                                   dos campos a serem extraídos.

        Returns:
            DataFrame: Uma cópia do DataFrame contendo apenas as colunas selecionadas.

        Raises:
            SystemExit: Se uma das colunas especificadas não existir no DataFrame.
        """
        try:
            name_fields = self.__get_name_fields(info)
            df_filtered = df[name_fields].copy()

        except Exception as error:
            print("[bold red]Erro ao transformar dados, verifique o log.[/bold red]")
            Log.error(f"Erro ao extrair colunas: {error}", True)
            raise SystemExit from error

        return df_filtered

    def __get_name_fields(self, info: dict[str, str]) -> list[str]:
        """Extrai e formata os nomes dos campos da configuração."""
        name_fields = []
        for field in info.keys():
            name_fields.append(field.lower())

        return name_fields

    def __rename(self, df: DataFrame, info: dict[str, str]) -> DataFrame:
        """
        Renomeia as colunas do DataFrame com base na configuração.

        Returns:
            DataFrame: O DataFrame com as colunas renomeadas.

        Raises:
            SystemExit: Se ocorrer um erro durante o processo de renomeação.
        """
        try:
            for field, value in info.items():
                df.rename(columns={field.lower(): value['field_destiny']}, inplace=True)

        except Exception as error:
            print("[bold red]Erro ao transformar dados, verifique o log.[/bold red]")
            Log.error(f"Erro ao renomear colunas: {error}", True)
            raise SystemExit from error

        return df

    def __transform_columns(self, df: DataFrame, info: dict[str, str]) -> DataFrame:
        """
        Aplica transformações de valor dinâmicas nas colunas.

        Itera sobre a configuração e, para cada transformação especificada,
        chama o método correspondente da classe `FieldHandler`.

        Returns:
            DataFrame: O DataFrame com os valores das colunas transformados.

        Raises:
            SystemExit: Se ocorrer um erro durante a aplicação de uma transformação.
        """

        for field, data in info.items():
            column = data['field_destiny']

            if 'transform' not in data:
                continue

            transforms = data['transform']
            for option, value in transforms.items():
                if value:
                    if hasattr(FieldHandler, option):
                        try:
                            transform_method = getattr(FieldHandler, option)
                            df = transform_method(df, column, option_data=value)

                        except Exception as error:
                            print("[bold red]Erro ao transformar dados, verifique o log.[/bold red]")
                            Log.error(f"Erro no processo de transformação: {error}", True)
                            raise SystemExit from error

        return df

    def __remove_columns(self, df: DataFrame, remove: dict[str, str]) -> DataFrame:
        """
        Remove colunas especificadas de um DataFrame.

        Args:
            DataFrame: O DataFrame a ser modificado.
            (list[str]): Uma lista com os nomes das colunas a serem removidas.

        Returns:
            DataFrame: O DataFrame sem as colunas removidas.

        Raises:
            SystemExit: Se uma coluna a ser removida não for encontrada.
        """
        for name, column in remove.items():
            try:
                del df[column]

            except Exception as error:
                print("[bold red]Erro ao transformar dados, verifique o log.[/bold red]")
                Log.error(f"Erro ao tentar remover coluna: {error}", True)
                raise SystemExit from error

        return df

    def __set_table(self, table: str ,df: DataFrame) -> None:
        """Adiciona um DataFrame processado a uma lista de dicionários de resultados."""
        self.__processed_data.append({table: df})
