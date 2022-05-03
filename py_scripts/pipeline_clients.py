# Загрузка данных о клиентах из таблицы-источника в схеме BANK в DWH

from py_scripts.utils import make_sql_query
from time import sleep


def clients_to_dwh(conn, logger):
    """
    Ф-ция выгружает файлы с данными о клиентах из таблицы-источника в схеме BANK в DWH.

    :param conn: коннектор к БД
    :param logger: логгер
    :return: None
    """

    logger.info(f"""
    Start parsing data: BANK.CLIENTS to DWH
    """)

    # Пауза необходима, т.к. в запросах идет сравнение по времени записей в БД, например, update_dt
    # Если цикл будет выполняться без паузы, время в некоторых случаях будет одинаковым, и запросы сработают некорректно
    pause_time = 3
    logger.info(f'Waiting {pause_time} seconds...')
    sleep(pause_time)

    # Выполнение пайплайна в SCD2

    # 1. Очистка стейджинга
    query = """
                    delete from demipt2.gold_stg_dim_clients
                    """
    make_sql_query(conn=conn, query=query, logger=logger)

    # 1.1 Очистка стейджинга с удаленными значениями
    query = """
                    delete from demipt2.gold_stg_dim_clients_del
                    """
    make_sql_query(conn=conn, query=query, logger=logger)

    # 2. Захват данных в стейджинг (кроме удалений)
    query = """
            insert into demipt2.gold_stg_dim_clients (
                client_id,
                last_name,
                first_name,
                patronymic,
                date_of_birth,
                passport_num,
                passport_valid_to,
                phone,
                create_dt,
                update_dt
            )
            select
                trim(client_id),
                trim(last_name),
                trim(first_name),
                trim(patronymic),
                date_of_birth,
                trim(passport_num),
                passport_valid_to,
                trim(phone),
                create_dt,
                case
                when update_dt is NULL then create_dt
                else current_date end as update_dt
            from bank.clients
            where 1=0
                or update_dt > (
                select coalesce( last_update_dt, to_date( '1900-01-01', 'yyyy-mm-dd') )
                from demipt2.gold_meta_bank where table_db = 'demipt2' and table_name = 'gold_dwh_dim_clients_hist' )
                or update_dt is null
            """
    make_sql_query(conn=conn, query=query, logger=logger)

    # 3. Захват ключей для вычисления удалений
    query = """
            insert into demipt2.gold_stg_dim_clients_del ( client_id )
            select client_id from bank.clients
            """
    make_sql_query(conn=conn, query=query, logger=logger)

    # 4. Выделяем "вставки" и "обновления" и вливаем их в приемник

    # 4.1 Вставка новой строки или закрытие текущей версии по scd2
    query = f"""
                    merge into demipt2.gold_dwh_dim_clients_hist tgt
                    using (
                        select
                            s.client_id,
                            s.last_name,
                            s.first_name,
                            s.patronymic,
                            s.date_of_birth,
                            s.passport_num,
                            s.passport_valid_to,
                            s.phone,
                            s.update_dt,
                            'n' as  deleted_flg,
                            s.update_dt as effective_from,
                            to_date( '9999-01-01', 'yyyy-mm-dd') as effective_to
                        from demipt2.gold_stg_dim_clients s
                        left join demipt2.gold_dwh_dim_clients_hist t
                        on s.client_id = t.client_id
                        and t.effective_to = to_date( '9999-01-01', 'yyyy-mm-dd' ) and deleted_flg = 'n'
                        where
                              t.client_id is null
                              or (
                                t.client_id is not null
                                and (1=0
                                        or (s.last_name <> t.last_name) or (s.last_name is null and t.last_name is not null) or (s.last_name is not null and t.last_name is null)
                                        or (s.first_name <> t.first_name) or (s.first_name is null and t.first_name is not null) or (s.first_name is not null and t.first_name is null)
                                        or (s.patronymic <> t.patronymic) or (s.patronymic is null and t.patronymic is not null) or (s.patronymic is not null and t.patronymic is null)
                                        or (s.date_of_birth <> t.date_of_birth) or (s.date_of_birth is null and t.date_of_birth is not null) or (s.date_of_birth is not null and t.date_of_birth is null)
                                        or (s.passport_num <> t.passport_num) or (s.passport_num is null and t.passport_num is not null) or (s.passport_num is not null and t.passport_num is null)
                                        or (s.passport_valid_to <> t.passport_valid_to) or (s.passport_valid_to is null and t.passport_valid_to is not null) or (s.passport_valid_to is not null and t.passport_valid_to is null) 
                                        or (s.phone <> t.phone) or (s.phone is null and t.phone is not null) or (s.phone is not null and t.phone is null)
                                    )
                                 )
                    ) stg
                    on ( tgt.client_id = stg.client_id )
                    when matched then
                    update set effective_to = current_date - interval '1' second
                    where effective_to = to_date( '9999-01-01', 'yyyy-mm-dd')
                    """
    make_sql_query(conn=conn, query=query, logger=logger)

    # 4.2 Вставка новой версии по scd2
    query = f"""
                    insert into demipt2.gold_dwh_dim_clients_hist (
                        client_id,
                        last_name,
                        first_name,
                        patronymic,
                        date_of_birth,
                        passport_num,
                        passport_valid_to,
                        phone,
                        deleted_flg,
                        effective_from,
                        effective_to
                    )
                    select
                        s.client_id,
                        s.last_name,
                        s.first_name,
                        s.patronymic,
                        s.date_of_birth,
                        s.passport_num,
                        s.passport_valid_to,
                        s.phone,
                        'n' as  deleted_flg,
                        current_date as effective_from,
                        to_date( '9999-01-01', 'yyyy-mm-dd') as effective_to
                    from demipt2.gold_stg_dim_clients s
                    left join demipt2.gold_dwh_dim_clients_hist t
                    on s.client_id = t.client_id
                    where
                          t.client_id is null
                          or (
                            t.client_id is not null
                            and (1=0
                                    or (s.last_name <> t.last_name) or (s.last_name is null and t.last_name is not null) or (s.last_name is not null and t.last_name is null)
                                    or (s.first_name <> t.first_name) or (s.first_name is null and t.first_name is not null) or (s.first_name is not null and t.first_name is null)
                                    or (s.patronymic <> t.patronymic) or (s.patronymic is null and t.patronymic is not null) or (s.patronymic is not null and t.patronymic is null)
                                    or (s.date_of_birth <> t.date_of_birth) or (s.date_of_birth is null and t.date_of_birth is not null) or (s.date_of_birth is not null and t.date_of_birth is null)
                                    or (s.passport_num <> t.passport_num) or (s.passport_num is null and t.passport_num is not null) or (s.passport_num is not null and t.passport_num is null)
                                    or (s.passport_valid_to <> t.passport_valid_to) or (s.passport_valid_to is null and t.passport_valid_to is not null) or (s.passport_valid_to is not null and t.passport_valid_to is null) 
                                    or (s.phone <> t.phone) or (s.phone is null and t.phone is not null) or (s.phone is not null and t.phone is null)
                                )
                             )
                      and effective_to <> to_date( '9999-01-01', 'yyyy-mm-dd')
                      and effective_to = (
                        select max(effective_to) 
                        from demipt2.gold_dwh_dim_clients_hist t1
                        where t.client_id = t1.client_id
                      )
                     """
    make_sql_query(conn=conn, query=query, logger=logger)

    # 5. Обновление информации об удалениях (для удаленных - флаг 'y')
    query = f"""
                update demipt2.gold_dwh_dim_clients_hist
                set 
                effective_to = current_date - interval '1' second,
                deleted_flg = 'y'
                where client_id in (
                select
                    t.client_id
                from demipt2.gold_dwh_dim_clients_hist t
                left join demipt2.gold_stg_dim_clients_del s
                on t.client_id = s.client_id
                where s.client_id is null
                )
                and deleted_flg = 'n'
                and effective_to = to_date( '9999-01-01', 'yyyy-mm-dd')
                """
    make_sql_query(conn=conn, query=query, logger=logger)

    # 6. Обновляем метаданные
    query = """
                update demipt2.gold_meta_bank
                set last_update_dt = ( select max(effective_from) from demipt2.gold_dwh_dim_clients_hist )
                where table_db = 'demipt2' and table_name = 'gold_dwh_dim_clients_hist' 
                and ( select max(effective_from) from demipt2.gold_dwh_dim_clients_hist ) is not null
            """
    make_sql_query(conn=conn, query=query, logger=logger)
