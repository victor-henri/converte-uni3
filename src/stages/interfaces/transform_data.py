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
