import os
import jaydebeapi
import cx_Oracle
import sqlalchemy
import pandas as pd

from sqlalchemy.exc import SQLAlchemyError

def get_sqlalchemy_connection(logger):
    '''
    Функция получения коннектора к БД Oracle через sqlalchemy и cx_Oracle
    :param logger: получение логгера
    :return: коннектор к БД
    '''

    try:
       engine = sqlalchemy.create_engine(
           "oracle+cx_oracle://demipt2:peregrintook@de-oracle.chronosavant.ru:1521/?service_name=deoracle", arraysize=1000
       )
       orders_sql = """SELECT * FROM BANK.ACCOUNTS"""
       df_orders = pd.read_sql(orders_sql, engine)
       logger.info(df_orders)
    except SQLAlchemyError as e:
       logger.info(f'Connection failed with Exception: {e}')

def get_jaydebeapi_connection(logger, path):
    '''
    Функция получения коннектора к БД Oracle через jaydebeapi
    :param logger: получение логгера
    :return: коннектор к БД
    '''

    path_to_jars = os.path.join(path, "jars", "ojdbc8.jar")

    try:
        conn = jaydebeapi.connect(
            'oracle.jdbc.driver.OracleDriver',
            'jdbc:oracle:thin:demipt2/peregrintook@de-oracle.chronosavant.ru:1521/deoracle',
            ['demipt2', 'peregrintook'],
            path_to_jars
        )
        conn.jconn.setAutoCommit(False)
    except Exception as e:
        logger.info(f'Setting connection failed with Exception: {e}')
    else:

        logger.info(f'Connection established! Params: {conn}')

    return conn


def check_connection(conn, logger):
    '''
    Проверяет коннект к БД, путем выполнения простого SQL-запроса

    :param conn: коннектор к Oracle
    :return: None
    '''

    query = "select 1 from DUAL"

    try:
        logger.info(f'Checking connection: {conn} by query: {query}')
        curs = conn.cursor()
        curs.execute("select 1 from DUAL")
    except Exception as e:
        logger.info(f'Connection {conn} is broken with Exception {e}')
    else:
        logger.info(f'Connection {conn} successfully checked')





