from time import sleep
from typer import run
from rich.progress import Progress, SpinnerColumn, TextColumn
from stages.extract.sql_extractor import Extractor
from stages.transform.transform_data import Transformer
from stages.load.load_data import Loader
from utils.connector import SQLConnector
from utils.config_json import JsonConfig


class MainPipeline:
    """
    Orquestra o fluxo completo do processo de ETL (Extract, Transform, Load).

    Esta classe atua como o ponto de entrada principal da aplicação, coordenando
    todas as etapas do pipeline em uma sequência lógica:
    1. Carregamento das configurações.
    2. Conexão com os bancos de dados de origem e destino.
    3. Extração (E) dos dados brutos.
    4. Transformação (T) e limpeza dos dados.
    5. Carga (L) dos dados processados no destino final.

    É projetada para ser executada de forma estática através do método `run()`.
    """

    __origin_conn: SQLConnector = SQLConnector()
    __destiny_conn: SQLConnector = SQLConnector()

    @classmethod
    def run(cls) -> None:
        """
        Executa a sequência completa de operações do pipeline de ETL.

        Este método é o ponto de partida que invoca todas as fases do processo,
        desde a leitura das configurações até a carga final dos dados.
        """
        with Progress(
            SpinnerColumn(spinner_name='boxBounce2'),
            TextColumn("[progress.description]{task.description}"),
            transient=False,
        ) as progress:

            task1 = progress.add_task(description="Verificando configurações...", total=1)
            tables = JsonConfig.get_tables()
            origin = JsonConfig.get_origin_db()
            destiny = JsonConfig.get_destiny_db()
            progress.update(task1, completed=1)

            task2 = progress.add_task(description="Conectando na origem dos dados...", total=1)
            cls.__origin_conn.db_connection(origin)
            origin_engine = cls.__origin_conn.get_engine()
            progress.update(task2, completed=1)

            task3 = progress.add_task(description="Conectando no destino dos dados...", total=1)
            cls.__destiny_conn.db_connection(destiny)
            destiny_engine = cls.__destiny_conn.get_engine()
            progress.update(task3, completed=1)

            task4 = progress.add_task(description="Extraindo dados...", total=1)
            extractor = Extractor(origin['font'], origin_engine)
            raw_data = extractor.extract(tables)
            progress.update(task4, completed=1)

            task5 = progress.add_task(description="Transformando dados...", total=1)
            transformer = Transformer(raw_data)
            clean_data = transformer.transform(tables)
            progress.update(task5, completed=1)

            task6 = progress.add_task(description="Carregando dados...", total=1)
            inserter = Loader(clean_data, destiny_engine)
            inserter.load()
            progress.update(task6, completed=1)

            task7 = progress.add_task(description="Processamento concluído com êxito...", total=1)
            progress.update(task7, completed=1)
            sleep(1)


if __name__ == "__main__":
    run(MainPipeline.run)
