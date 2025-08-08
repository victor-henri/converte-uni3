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
