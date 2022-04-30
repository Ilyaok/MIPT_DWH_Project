import os
import pandas as pd

from py_scripts.terminals_STG_pipeline import terminals_to_staging
from py_scripts.logger import create_logger
from py_scripts.utils import get_connection


def main():

    cwd = os.getcwd()

    logger = create_logger(cwd)
    logger.info(f'Starting {cwd}/{__name__}')

    # Блог присоединения к БД Oracle
    logger.info(f'Connecting to Oracle...')

    try:
        conn = get_connection()
    except Exception as e:
        logger.info(f'Connection failed with Exception: {e}')
    finally:
        logger.info(f'Connected! Params: {conn}')

    terminals_to_staging(conn, cwd, logger)

    # conn.close()

    logger.info(f'Connection closed')


if __name__ == "__main__":
    main()