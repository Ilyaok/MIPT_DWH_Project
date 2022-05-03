# Загрузка данных о картах из таблицы-источника в схеме BANK в DWH

from py_scripts.utils import make_sql_query
from time import sleep

def cards_to_dwh(conn, logger):
    """
    Ф-ция выгружает файлы с данными о картаъ из таблицы-источника в схеме BANK в DWH.

    :param conn: коннектор к БД
    :param logger: логгер
    :return: None
    """

    logger.info(f"""
    Start parsing data: BANK.CARDS to DWH
    """)

    # Пауза необходима, т.к. в запросах идет сравнение по времени записей в БД, например, update_dt
    # Если цикл будет выполняться без паузы, время в некоторых случаях будет одинаковым, и запросы сработают некорректно
    pause_time = 3
    logger.info(f'Waiting {pause_time} seconds...')
    sleep(pause_time)

    # Выполнение пайплайна в SCD2

    # 1. Очистка стейджинга
    query = """
                    delete from demipt2.gold_stg_dim_cards
                    """
    make_sql_query(conn=conn, query=query, logger=logger)

    # 1.1 Очистка стейджинга с удаленными значениями
    query = """
                    delete from demipt2.gold_stg_dim_cards_del
                    """
    make_sql_query(conn=conn, query=query, logger=logger)

    # 2. Захват данных в стейджинг (кроме удалений)
    query = """
            insert into demipt2.gold_stg_dim_cards (
                card_num,
                account_num,
                create_dt,
                update_dt
            )
            select
                trim(card_num),
                trim(account),
                create_dt,
                case
                when update_dt is NULL then create_dt
                else current_date end as update_dt
            from bank.cards
            where 1=0
                or update_dt > (
                select coalesce( last_update_dt, to_date( '1900-01-01', 'yyyy-mm-dd') )
                from demipt2.gold_meta_bank where table_db = 'demipt2' and table_name = 'gold_dwh_dim_cards_hist' )
                or update_dt is null
            """
    make_sql_query(conn=conn, query=query, logger=logger)

    # 3. Захват ключей для вычисления удалений
    query = """
            insert into demipt2.gold_stg_dim_cards_del ( card_num )
            select card_num from bank.cards
            """
    make_sql_query(conn=conn, query=query, logger=logger)

    # 4. Выделяем "вставки" и "обновления" и вливаем их в приемник

    # 4.1 Вставка новой строки или закрытие текущей версии по scd2
    query = f"""
            merge into demipt2.gold_dwh_dim_cards_hist tgt
            using (
                select
                    s.card_num,
                    s.account_num,
                    s.update_dt,
                    'n' as  deleted_flg,
                    s.update_dt as effective_from,
                    to_date( '9999-01-01', 'yyyy-mm-dd') as effective_to
                from demipt2.gold_stg_dim_cards s
                left join demipt2.gold_dwh_dim_cards_hist t
                on s.card_num = t.card_num
                and t.effective_to = to_date( '9999-01-01', 'yyyy-mm-dd' ) and deleted_flg = 'n'
                where
                    t.card_num is null
                      or (
                        t.card_num is not null
                        and (1=0
                                 or (s.account_num <> t.account_num) or (s.account_num is null and t.account_num is not null)
                                 or (s.account_num is not null and t.account_num is null)
                            )
                         )
            ) stg
            on ( tgt.card_num = stg.card_num )
            when matched then
            update set effective_to = current_date - interval '1' second
            where effective_to = to_date( '9999-01-01', 'yyyy-mm-dd')
                    """
    make_sql_query(conn=conn, query=query, logger=logger)

    # 4.2 Вставка новой версии по scd2
    query = f"""
                    insert into demipt2.gold_dwh_dim_cards_hist (
                        card_num,
                        account_num,
                        deleted_flg,
                        effective_from,
                        effective_to
                    )
                    select
                        s.card_num,
                        s.account_num,
                        'n' as  deleted_flg,
                        current_date as effective_from,
                        to_date( '9999-01-01', 'yyyy-mm-dd') as effective_to
                    from demipt2.gold_stg_dim_cards s
                    left join demipt2.gold_dwh_dim_cards_hist t
                    on s.card_num = t.card_num
                    where
                          t.card_num is null
                          or (
                            t.card_num is not null
                            and (1=0
                                     or (s.account_num <> t.account_num) or (s.account_num is null and t.account_num is not null)
                                     or (s.account_num is not null and t.account_num is null)
                                )
                             )
                      and effective_to <> to_date( '9999-01-01', 'yyyy-mm-dd')
                      and effective_to = (
                        select max(effective_to) 
                        from demipt2.gold_dwh_dim_cards_hist t1
                        where t.card_num = t1.card_num
                      )
                     """
    make_sql_query(conn=conn, query=query, logger=logger)

    # 5. Обновление информации об удалениях (для удаленных - флаг 'y')
    query = f"""
                update demipt2.gold_dwh_dim_cards_hist
                set 
                effective_to = current_date - interval '1' second,
                deleted_flg = 'y'
                where card_num in (
                select
                    t.card_num
                from demipt2.gold_dwh_dim_cards_hist t
                left join demipt2.gold_stg_dim_cards_del s
                on t.card_num = s.card_num
                where s.card_num is null
                )
                and deleted_flg = 'n'
                and effective_to = to_date( '9999-01-01', 'yyyy-mm-dd')
                """
    make_sql_query(conn=conn, query=query, logger=logger)

    # 6. Обновляем метаданные
    query = """
                update demipt2.gold_meta_bank
                set last_update_dt = ( select max(effective_from) from demipt2.gold_dwh_dim_cards_hist )
                where table_db = 'demipt2' and table_name = 'gold_dwh_dim_cards_hist' 
                and ( select max(effective_from) from demipt2.gold_dwh_dim_cards_hist ) is not null
            """
    make_sql_query(conn=conn, query=query, logger=logger)
