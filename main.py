import pandas as pd
import jaydebeapi
import logging
import multiprocessing


# Блок создания логгера
log = logging.getLogger()

logger = multiprocessing.get_logger()
logger.setLevel(logging.INFO)

for handler in logger.handlers[:]:
    logger.removeHandler(handler)

sh = logging.StreamHandler()
fh = logging.FileHandler('logs/log_process.log')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
sh.setFormatter(formatter)
fh.setFormatter(formatter)
logger.addHandler(sh)
logger.addHandler(fh)


def main():
    logger.info(f'Starting {__name__}')

    # Блог присоединения к БД Oracle
    logger.info(f'Connecting to Oracle...')

    conn = jaydebeapi.connect(
        'oracle.jdbc.driver.OracleDriver',
        'jdbc:oracle:thin:demipt2/peregrintook@de-oracle.chronosavant.ru:1521/deoracle',
        ['demipt2', 'peregrintook'],
        'jars/ojdbc8.jar'
    )

    conn.jconn.setAutoCommit(False)
    curs = conn.cursor()

    logger.info(f'Connected! Params: {conn, curs}')

    # Получение текста запроса из sql-файла (с подстановкой имен таблиц в форматированную строку)
    with open("sql_scripts/test_pipeline.sql") as sql_file:
        query = sql_file.read()

    query = query.format(
        source_table='bank.accounts')

    curs.execute(query)
    conn.commit()
    df_test = curs.fetchall()

    names = [ x[0] for x in curs.description ]
    df_test = pd.DataFrame(result, columns=names)
    print(df_test)

    # Блок выгрузки таблиц из Oracle в Dataframes
    # curs.execute("SELECT * FROM BANK.ACCOUNTS")
    # conn.commit()
    # df_banks = curs.fetchall()
    #
    # curs.execute("SELECT * FROM BANK.CARDS")
    # conn.commit()
    # df_cards = curs.fetchall()
    #
    # curs.execute("SELECT * FROM BANK.CLIENTS")
    # conn.commit()
    # df_clients = curs.fetchall()

    # names = [ x[0] for x in curs.description ]
    # df_banks = pd.DataFrame(result, columns=names)
    # print(df_banks)
    # print(df_cards)
    # print(df_clients)

    conn.close()

    logger.info(f'Connection closed')


if __name__ == "__main__":
    main()