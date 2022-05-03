# Создание отчета по мошенническим операциям

from py_scripts.utils import make_sql_query
from time import sleep


def create_fraud_report(conn, logger):
    """
    Ф-ция создает отчет по мошенническим операциям и помещает его в таблицу demipt2.gold_rep_fraud

    :param conn: коннектор к БД
    :param logger: логгер
    :return: None
    """

    logger.info(f"""
       Creating fraud-report...
    """)

    # Очистка предыдущей версии отчета
    query = """
                    delete from demipt2.gold_rep_fraud
                    """
    make_sql_query(conn=conn, query=query, logger=logger)

    # Создание отчета
    query = """
                insert into demipt2.gold_rep_fraud (event_dt, passport, fio, phone, event_type, report_dt) 
                values (to_date( '2022-03-01', 'yyyy-mm-dd'), 'passport_num', 'f.i.o', '112233', 'SUCCESS', current_date)
            """
    make_sql_query(conn=conn, query=query, logger=logger)

    logger.info(f'Fraud-report created in the table: demipt2.gold_rep_fraud')
