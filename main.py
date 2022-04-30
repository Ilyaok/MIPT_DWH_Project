import os
import pandas as pd

from py_scripts.terminals_STG_pipeline import terminals_to_staging
from py_scripts.logger import create_logger
from py_scripts.utils import get_connection, check_connection


def main():

    path_to_project = os.getcwd()

    logger = create_logger(path_to_project)
    logger.info(f'Starting {path_to_project}/{__name__}')

    # Блог присоединения к БД Oracle
    logger.info(f'Connecting to Oracle...')

    conn = get_connection(logger, path_to_project)
    check_connection(conn, logger)

    terminals_to_staging(conn, path_to_project, logger)

    conn.close()

    logger.info(f'Connection closed')


if __name__ == "__main__":
    main()