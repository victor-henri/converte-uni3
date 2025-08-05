from abc import ABC, abstractmethod


class LoadInterface(ABC):
    """Define a interface para classes de Carga (Load) de dados."""

    @abstractmethod
    def load(self):
        """
        Define o contrato para executar o processo de carga dos dados.
        """
        raise NotImplementedError
