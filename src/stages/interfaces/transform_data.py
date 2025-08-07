from abc import ABC, abstractmethod
from stages.contracts.transform_contract import TransformContract


class TransformInterface(ABC):
    """Define a interface para classes de transformação de dados."""

    @abstractmethod
    def transform(self, tables: dict[str, str]) -> TransformContract:
        """
        Define o contrato para executar o processo de transformação.

        Args:
            tables (dict[str, str]): Dicionário de configuração que guia como
                                     cada conjunto de dados deve ser transformado.

        Returns:
            TransformContract: Um objeto de contrato contendo os dados limpos.
        """
        raise NotImplementedError
