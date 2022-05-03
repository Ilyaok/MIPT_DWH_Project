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

    # Пауза необходима, т.к. в запросах идет сравнение по времени записей в БД, например, update_dt
    # Если цикл будет выполняться без паузы, время в некоторых случаях будет одинаковым, и запросы сработают некорректно
    pause_time = 3
    logger.info(f'Waiting {pause_time} seconds...')
    sleep(pause_time)

    # Формирование отчета о мошеннических операциях - demipt2.gold_rep_fraud

    # 1. Вставка данных об операциях с просроченными или заблокированными паспортами
    query = """
            merge into demipt2.gold_rep_fraud t1
            using (
                select
                    transactions.trans_date as event_dt,
                    clients.passport_num as passport,
                    clients.last_name || ' ' || clients.first_name || ' ' || clients.patronymic as fio,
                    clients.phone as phone,
                    1 as event_type,
                    current_date as report_dt
                from demipt2.gold_dwh_fact_transactions transactions
                         left join demipt2.gold_dwh_dim_terminals_hist terminals on transactions.terminal = terminals.terminal_id
                         left join demipt2.gold_dwh_dim_cards_hist cards on transactions.card_num = cards.card_num
                         left join demipt2.gold_dwh_dim_accounts_hist accounts on cards.account_num = accounts.account_num
                         left join demipt2.gold_dwh_dim_clients_hist clients on accounts.client = clients.client_id
                         left join demipt2.gold_dwh_fact_pssprt_blcklst pssprt_blcklst on clients.passport_num = pssprt_blcklst.passport_num
                where
                    1=1
                    and (
                        1=1
                        and pssprt_blcklst.passport_num is not null
                        and pssprt_blcklst.entry_dt is not null
                        and transactions.trans_date >= pssprt_blcklst.entry_dt
                        )
                    or  (
                        1=1
                        and clients.passport_valid_to is not null
                        and transactions.trans_date >= clients.passport_valid_to
                        )
            ) t2
            on (1 = 1
                and t1.event_dt = t2.event_dt
                and t1.passport = t2.passport
                and t1.fio = t2.fio
                and t1.phone = t2.phone
                and t1.event_type = t2.event_type
                )
            when not matched then
                insert values (
                                t2.event_dt,
                                t2.passport,
                                t2.fio,
                                t2.phone,
                                t2.event_type,
                                t2.report_dt
                              )
            """
    make_sql_query(conn=conn, query=query, logger=logger)

    # 2. Совершение операции при недействующем договоре.
    query = """
            merge into demipt2.gold_rep_fraud t1
            using (
                select
                    transactions.trans_date as event_dt,
                    clients.passport_num as passport,
                    clients.last_name || ' ' || clients.first_name || ' ' || clients.patronymic as fio,
                    clients.phone as phone,
                    1 as event_type,
                    current_date as report_dt
                from demipt2.gold_dwh_fact_transactions transactions
                         left join demipt2.gold_dwh_dim_terminals_hist terminals on transactions.terminal = terminals.terminal_id
                         left join demipt2.gold_dwh_dim_cards_hist cards on transactions.card_num = cards.card_num
                         left join demipt2.gold_dwh_dim_accounts_hist accounts on cards.account_num = accounts.account_num
                         left join demipt2.gold_dwh_dim_clients_hist clients on accounts.client = clients.client_id
                         left join demipt2.gold_dwh_fact_pssprt_blcklst pssprt_blcklst on clients.passport_num = pssprt_blcklst.passport_num
                where
                    1=1
                    and (
                        1=1
                        and accounts.valid_to is not null
                        and transactions.trans_date >= accounts.valid_to
                        )
            ) t2
            on (1 = 1
                and t1.event_dt = t2.event_dt
                and t1.passport = t2.passport
                and t1.fio = t2.fio
                and t1.phone = t2.phone
                and t1.event_type = t2.event_type
                )
            when not matched then
                insert values (
                                t2.event_dt,
                                t2.passport,
                                t2.fio,
                                t2.phone,
                                t2.event_type,
                                t2.report_dt
                              )
            """
    make_sql_query(conn=conn, query=query, logger=logger)

    # 3. Совершение операций в разных городах в течение одного часа
    query = """
            merge into demipt2.gold_rep_fraud t3
            using (
                    select
                        t1.t1_event_dt as event_dt,
                        t1.t1_passport as passport,
                        t1.t1_fio as fio,
                        t1.t1_phone as phone,
                        t1.t1_event_type as event_type,
                        t1_report_dt as report_dt
                    from
                        (
                            select
                                transactions.TRANS_DATE as t1_event_dt,
                                clients.PASSPORT_NUM as t1_passport,
                                clients.last_name || ' ' || clients.first_name || ' ' || clients.patronymic as t1_fio,
                                clients.phone as t1_phone,
                                3 as t1_event_type,
                                current_date as t1_report_dt,
                                transactions.CARD_NUM as t1_card_num,
                                transactions.TRANS_DATE as t1_trans_date,
                                terminals.TERMINAL_CITY as t1_term_city
                            from demipt2.gold_dwh_fact_transactions transactions
                                 left join demipt2.gold_dwh_dim_terminals_hist terminals on transactions.terminal = terminals.terminal_id
                                 left join demipt2.gold_dwh_dim_cards_hist cards on transactions.card_num = cards.card_num
                                 left join demipt2.gold_dwh_dim_accounts_hist accounts on cards.account_num = accounts.account_num
                                 left join demipt2.gold_dwh_dim_clients_hist clients on accounts.client = clients.client_id
                                 left join demipt2.gold_dwh_fact_pssprt_blcklst pssprt_blcklst
                             on clients.passport_num = pssprt_blcklst.passport_num
                             ) t1
                    inner join (
                            select
                                transactions.TRANS_DATE as t2_event_dt,
                                clients.PASSPORT_NUM as t2_passport,
                                clients.last_name || ' ' || clients.first_name || ' ' || clients.patronymic as t2_fio,
                                clients.phone as t2_phone,
                                3 as t2_event_type,
                                current_date as t2_report_dt,
                                transactions.CARD_NUM as t2_card_num,
                                transactions.TRANS_DATE as t2_trans_date,
                                terminals.TERMINAL_CITY as t2_term_city
                            from demipt2.gold_dwh_fact_transactions transactions
                                 left join demipt2.gold_dwh_dim_terminals_hist terminals on transactions.terminal = terminals.terminal_id
                                 left join demipt2.gold_dwh_dim_cards_hist cards on transactions.card_num = cards.card_num
                                 left join demipt2.gold_dwh_dim_accounts_hist accounts on cards.account_num = accounts.account_num
                                 left join demipt2.gold_dwh_dim_clients_hist clients on accounts.client = clients.client_id
                                 left join demipt2.gold_dwh_fact_pssprt_blcklst pssprt_blcklst
                             on clients.passport_num = pssprt_blcklst.passport_num
                             ) t2
                    on (
                        1=1
                        and t1.t1_card_num = t2.t2_card_num
                        and t2.t2_trans_date > t1.t1_trans_date
                        and (24 * (t2.t2_trans_date - t1.t1_trans_date) <= 1)
                        and (t1.t1_term_city <> t2.t2_term_city)
                        )
                    ) t4
            on (1 = 1
                and t3.event_dt = t4.event_dt
                and t3.passport = t4.passport
                and t3.fio = t4.fio
                and t3.phone = t4.phone
                and t3.event_type = t4.event_type
                )
            when not matched then
                insert values (
                                t4.event_dt,
                                t4.passport,
                                t4.fio,
                                t4.phone,
                                t4.event_type,
                                t4.report_dt
                              )
    """
    make_sql_query(conn=conn, query=query, logger=logger)

    logger.info(f'Fraud-report created in the table: demipt2.gold_rep_fraud')
