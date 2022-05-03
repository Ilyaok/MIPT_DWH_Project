#!/usr/bin/python

import os

from py_scripts.pipeline_terminals import terminals_to_dwh
from py_scripts.pipeline_passport_blacklist import passport_blacklist_to_dwh
from py_scripts.pipeline_transactions import transactions_to_dwh
from py_scripts.pipeline_accounts import accounts_to_dwh
from py_scripts.pipeline_cards import cards_to_dwh
from py_scripts.pipeline_clients import clients_to_dwh
from py_scripts.create_fraud_report import create_fraud_report
from py_scripts.logger import create_logger
from py_scripts.utils import get_jaydebeapi_connection, check_connection


def main():
    path_to_project = os.getcwd()

    logger = create_logger()
    logger.info(f'Starting {path_to_project}/{__name__}')
    logger.info(f'Connecting to Oracle...')

    conn = get_jaydebeapi_connection(path=path_to_project, logger=logger)
    check_connection(conn, logger)

    # Загрузка данных о терминалах из Excel-файлов в DWH
    terminals_to_dwh(conn=conn, path=path_to_project, logger=logger)

    # Загрузка данных о черном списке паспортов из Excel-файлов в DWH
    passport_blacklist_to_dwh(conn=conn, path=path_to_project, logger=logger)

    # Загрузка данных о транзакциях из txt-файлов в DWH
    transactions_to_dwh(conn=conn, path=path_to_project, logger=logger)

    # Загрузка данных об аккаунтах из таблицы-источника в схеме BANK в DWH
    accounts_to_dwh(conn, logger)

    # Загрузка данных о картах из таблицы-источника в схеме BANK в DWH
    cards_to_dwh(conn, logger)

    # Загрузка данных о клиентах из таблицы-источника в схеме BANK в DWH
    clients_to_dwh(conn, logger)

    # Создание отчета о мошеннических операциях
    create_fraud_report(conn, logger)

    conn.close()
    logger.info(f'Connection closed')


if __name__ == "__main__":
    main()

