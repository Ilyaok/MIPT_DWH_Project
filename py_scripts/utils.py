# import jaydebeapi

def get_connection():
    '''
    Функция получения коннектора к БД Oracle через jaydebeapi
    :param logger:
    :return: коннектор к БД
    '''

    # conn = jaydebeapi.connect(
    #     'oracle.jdbc.driver.OracleDriver',
    #     'jdbc:oracle:thin:demipt2/peregrintook@de-oracle.chronosavant.ru:1521/deoracle',
    #     ['demipt2', 'peregrintook'],
    #     'jars/ojdbc8.jar'
    # )
    #
    # conn.jconn.setAutoCommit(False)
    #
    # return conn

    return 'conn'

