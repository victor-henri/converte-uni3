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

import logging
import inspect


class Log:
    """
    Fornece uma interface de logging centralizada e pré-configurada para a aplicação.

    Esta classe utiliza o módulo `logging` do Python para registrar mensagens
    em um arquivo chamado 'app.log'. Ela oferece métodos estáticos para diferentes
    níveis de severidade (INFO, WARNING, ERROR, CRITICAL).

    Características principais:
    - Logs de erro e críticos incluem automaticamente informações de contexto
      (módulo, função e linha) de onde foram chamados.
    - Um separador é adicionado após cada mensagem para facilitar a leitura do arquivo.
    - A configuração do logger (nível, formatadores, handler) é feita
      internamente, abstraindo a complexidade do usuário final.
    """

    __logger: str = logging.getLogger(__name__)
    __logger.setLevel("INFO")

    __separator: str = logging.Formatter("%(message)s", style="%")

    __simple_formatter: str = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s",
        style="%",
        datefmt="%d-%m-%Y %H:%M"
    )

    __file_handler: str = logging.FileHandler("app.log", mode="a")
    __logger.addHandler(__file_handler)

    @classmethod
    def __custom_formatter(cls, mod: str, func: str, line: str) -> str:
        """
        Cria um formatador de log detalhado para mensagens de erro e críticas.

        Args:
            mod (str): O nome do módulo onde o log foi originado.
            func (str): O nome da função onde o log foi originado.
            line (str): O número da linha onde o log foi originado.

        Returns:
            logging.Formatter: Um objeto formatador com informações de contexto.
        """
        return logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s\n"
            f"Module: {mod}\nFunction: {func}\nLine: {line}",
            datefmt="%d-%m-%Y %H:%M"
        )

    @classmethod
    def __print_separator(cls) -> None:
        """Adiciona uma linha separadora ao arquivo de log para melhor legibilidade."""
        cls.__file_handler.setFormatter(cls.__separator)
        cls.__logger.info("================")

    @classmethod
    def info(cls, message: str) -> None:
        """
        Registra uma mensagem com o nível INFO.

        Ideal para registrar eventos gerais da aplicação e marcos de progresso.

        Args:
            message (str): A mensagem a ser registrada.
        """
        cls.__file_handler.setFormatter(cls.__simple_formatter)
        cls.__logger.info(message)
        cls.__print_separator()

    @classmethod
    def warning(cls, message: str) -> None:
        """
        Registra uma mensagem com o nível WARNING.

        Usado para indicar que algo inesperado aconteceu, mas que não impede
        o funcionamento normal da aplicação (ex: uso de uma API obsoleta).

        Args:
            message (str): A mensagem de aviso a ser registrada.
        """
        cls.__file_handler.setFormatter(cls.__simple_formatter)
        cls.__logger.warning(message)
        cls.__print_separator()

    @classmethod
    def error(cls, message: str, trace: bool = False) -> None:
        """
        Registra uma mensagem com o nível ERROR.

        Indica um problema sério que impediu a execução de uma operação específica,
        mas não necessariamente da aplicação inteira. Inclui automaticamente o
        contexto da chamada (módulo, função, linha).

        Args:
            message (str): A mensagem de erro a ser registrada.
            trace (bool): Se True, inclui o traceback completo da exceção no log.
                          Útil para depuração detalhada. O padrão é False.
        """
        caller_frame = inspect.currentframe().f_back
        module_name = inspect.getmodule(caller_frame).__name__
        func_name = caller_frame.f_code.co_name
        line_no = caller_frame.f_lineno

        custom_formatter = cls.__custom_formatter(module_name, func_name, line_no)

        cls.__file_handler.setFormatter(custom_formatter)
        cls.__logger.error(message, exc_info=trace)
        cls.__print_separator()

    @classmethod
    def critical(cls, message: str, trace: bool = False) -> None:
        """
        Registra uma mensagem com o nível CRITICAL.

        Indica um erro gravíssimo que provavelmente levará ao encerramento da
        aplicação. Inclui automaticamente o contexto da chamada.

        Args:
            message (str): A mensagem crítica a ser registrada.
            trace (bool): Se True, inclui o traceback completo da exceção no log.
                          O padrão é False.
        """
        caller_frame = inspect.currentframe().f_back
        module_name = inspect.getmodule(caller_frame).__name__
        func_name = caller_frame.f_code.co_name
        line_no = caller_frame.f_lineno

        custom_formatter = cls.__custom_formatter(module_name, func_name, line_no)

        cls.__file_handler.setFormatter(custom_formatter)
        cls.__logger.critical(message, exc_info=trace)
        cls.__print_separator()
