import os
import jaydebeapi
import shutil

def get_jaydebeapi_connection(path, logger):
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


def make_sql_query(conn, query, logger):
    '''
    Выполняет SQL-запрос для заданного пайплайна

    :param conn: коннектор к БД
    :param query: SQL-запрос
    :param logger: логгер
    :return:
    '''

    logger.info(f'Making SQL-query:\n{query}')
    try:
        curs = conn.cursor()
        curs.execute(query)
        conn.commit()
    except Exception as e:
        logger.info(f'Failed to perform query with Exception {e}')
        try:
            curs.execute('rollback')
            logger.info('Rollback performed!')
            exit()
        except Exception as e:
            logger.info(f'Rollback not performed with Exception {e}')
            exit()
    else:
        logger.info(f'Query performed')


def rename_and_move_file(source_path, target_path, logger):
    try:
        shutil.move(source_path, target_path)
        os.rename(target_path, target_path + '.backup')
    except Exception as e:
        logger.info(f'''File {source_path} wasn't backed up with Exception {e}''')
    else:
        logger.info(f'''File {source_path} was successfully backed up to {target_path}''')





