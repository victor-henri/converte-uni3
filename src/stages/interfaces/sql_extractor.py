from abc import ABC, abstractmethod
from stages.contracts.extract_contract import ExtractContract


class ExtractInterface(ABC):
    """
    Define a interface para extratores de dados.
    
    O objetivo desta interface é garantir que qualquer extrator,
    independentemente da fonte, siga um contrato de método único e consistente.
    """

    @abstractmethod
    def extract(self, tables: dict[str, str]) -> ExtractContract:
        """
        Define o contrato para extrair dados de uma ou mais tabelas/entidades.

        Args:
            tables (dict[str, dict]): Dicionário que mapeia um nome lógico 
                                      para as especificações da tabela a ser extraída.

        Returns:
            ExtractContract: Um objeto de contrato contendo os dados brutos 
                             extraídos e metadados da extração.
        """
        raise NotImplementedError
