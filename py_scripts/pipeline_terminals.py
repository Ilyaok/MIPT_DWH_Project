# Загрузка данных о терминалах из Excel-файлов в DWH

import pandas as pd
import os
import re

from py_scripts.utils import rename_and_move_file, make_sql_query
from time import sleep


def terminals_to_dwh(conn, path, logger):
    """
    Ф-ция забирает файлы с данными о терминалах по заданному паттерну.
    Если файлов несколько, прогружает их последовательно, согласно дате в именах файлов по паттерну из задания.

    :param conn: коннектор к БД
    :param path: путь расположению файлов
    :param logger: логгер
    :return: None
    """

    # Создадим список всех файлов по паттерну terminals_DDMMYYYY.xlsx
    pattern = r'terminals_[0-9]+.\.xlsx'

    logger.info(f"""
    Start parsing data: terminals. Path: {path} Pattern: {pattern}
    """)

    filenames_list = [f for f in os.listdir(path) if re.match(pattern, f)]
    filenames_list.sort()

    if len(filenames_list) == 0:
        logger.info(f'No files matching pattern detected. Pattern: {pattern}')
        return 0
    logger.info(f'List of files for processing: {filenames_list}')

    for filename in filenames_list:
        # Сформируем датафрейм
        logger.info(f'Start processing file: {filename}')

        # Пауза необходима, т.к. в запросах идет сравнение по времени записей в БД, например, update_dt
        # Если цикл будет выполняться без паузы, время в некоторых случаях будет одинаковым, и запросы сработают некорректно
        pause_time = 3
        logger.info(f'Waiting {pause_time} seconds...')
        sleep( pause_time)

        df = pd.read_excel(os.path.join(path, filename))

        logger.info('Created dataframe:')
        logger.info(f'\n{df.to_string()}')

        # Запишем датафрейм в слой сырых данных demipt2.gold_stg_dim_terminals_raw

        # Очистка предыдущей версии таблицы-источника в DWH
        query = """
                delete from demipt2.gold_stg_dim_terminals_raw
                """
        make_sql_query(conn=conn, query=query, logger=logger)

        curs = conn.cursor()

        # Запись датафрейма в demipt2.gold_stg_dim_terminals_raw
        try:
            query = f"""
                insert into demipt2.gold_stg_dim_terminals_raw (
                    terminal_id,
                    terminal_type,
                    terminal_city,
                    terminal_address,
                    update_dt)
                values (?,?,?,?,current_date)
                """
            logger.info('Insert data into demipt2.gold_stg_dim_terminals_raw with query:\n')
            logger.info(query)
            curs.executemany(query, df.values.tolist())
            logger.info('Data was inserted into demipt2.gold_stg_dim_terminals_raw!')
        except Exception as e:
            logger.info(f'Data was not inserted into demipt2.gold_stg_dim_terminals_raw! Exception: {e}')
            exit()

        conn.commit()
        curs.close()

        # Выполнение пайплайна в SCD2

        # 1. Очистка стейджинга
        query = """
                delete from demipt2.gold_stg_dim_terminals
                """
        make_sql_query(conn=conn, query=query, logger=logger)

        # 1.1 Очистка стейджинга с удаленными значениями
        query = """
                delete from demipt2.gold_stg_dim_terminals_del
                """
        make_sql_query(conn=conn, query=query, logger=logger)

        # 2. Захват данных в стейджинг (кроме удалений)
        query = """
            insert into demipt2.gold_stg_dim_terminals (
                terminal_id,
                terminal_type,
                terminal_city,
                terminal_address,
                update_dt
            )
            select
                trim(terminal_id),
                trim(terminal_type),
                trim(terminal_city),
                trim(terminal_address),
                update_dt
            from demipt2.gold_stg_dim_terminals_raw
            where update_dt > (
                select coalesce( last_update_dt, to_date( '1900-01-01', 'yyyy-mm-dd') )
                from demipt2.gold_meta_bank 
                where table_db = 'demipt2' and table_name = 'gold_dwh_dim_terminals_hist' )
                or update_dt is null
                """
        make_sql_query(conn=conn, query=query, logger=logger)

        # 3. Захват ключей для вычисления удалений
        query = """
                insert into demipt2.gold_stg_dim_terminals_del ( terminal_id )
                select terminal_id from demipt2.gold_stg_dim_terminals_raw
                """
        make_sql_query(conn=conn, query=query, logger=logger)

        # 4. Выделяем "вставки" и "обновления" и вливаем их в приемник

        # 4.1 Вставка новой строки или закрытие текущей версии по scd2
        query = f"""
                merge into demipt2.gold_dwh_dim_terminals_hist tgt
                using (
                    select
                        s.terminal_id,
                        s.terminal_type,
                        s.terminal_city,
                        s.terminal_address,
                        s.update_dt,
                        'n' as  deleted_flg,
                        s.update_dt as effective_from,
                        to_date( '9999-01-01', 'yyyy-mm-dd') as effective_to
                    from demipt2.gold_stg_dim_terminals s
                    left join demipt2.gold_dwh_dim_terminals_hist t
                    on s.terminal_id = t.terminal_id
                    and t.effective_to = to_date( '9999-01-01', 'yyyy-mm-dd' ) and deleted_flg = 'n'
                    where
                      t.terminal_id is null
                      or (
                      t.terminal_id is not null
                      and ( 1=0
                        or (s.terminal_type <> t.terminal_type) or (s.terminal_type is null and t.terminal_type is not null)
                        or (s.terminal_type is not null and t.terminal_type is null)
                        or (s.terminal_city <> t.terminal_city) or (s.terminal_city is null and t.terminal_city is not null)
                        or (s.terminal_city is not null and t.terminal_city is null)
                        or (s.terminal_address <> t.terminal_address) or (s.terminal_address is null and t.terminal_address is not null)
                        or (s.terminal_address is not null and t.terminal_address is null)
                        )
                      )
                ) stg
                on ( tgt.terminal_id = stg.terminal_id )
                when matched then
                update set effective_to = current_date - interval '1' second
                where effective_to = to_date( '9999-01-01', 'yyyy-mm-dd')
                """
        make_sql_query(conn=conn, query=query, logger=logger)

        # 4.2 Вставка новой версии по scd2
        query = f"""
                insert into demipt2.gold_dwh_dim_terminals_hist (
                    terminal_id,
                    terminal_type,
                    terminal_city,
                    terminal_address,
                    deleted_flg,
                    effective_from,
                    effective_to
                )
                select
                    s.terminal_id,
                    s.terminal_type,
                    s.terminal_city,
                    s.terminal_address,
                    'n' as  deleted_flg,
                    current_date as effective_from,
                    to_date( '9999-01-01', 'yyyy-mm-dd') as effective_to
                from demipt2.gold_stg_dim_terminals s
                left join demipt2.gold_dwh_dim_terminals_hist t
                on s.terminal_id = t.terminal_id 
                where
                  t.terminal_id is null
                  or 
                  (
                    t.terminal_id is not null
                    and (   1=0
                            or (s.terminal_type <> t.terminal_type) or (s.terminal_type is null and t.terminal_type is not null)
                            or (s.terminal_type is not null and t.terminal_type is null)
                            or (s.terminal_city <> t.terminal_city) or (s.terminal_city is null and t.terminal_city is not null)
                            or (s.terminal_city is not null and t.terminal_city is null)
                            or (s.terminal_address <> t.terminal_address) or (s.terminal_address is null and t.terminal_address is not null)
                            or (s.terminal_address is not null and t.terminal_address is null)
                        )
                  )
                  and effective_to <> to_date( '9999-01-01', 'yyyy-mm-dd')
                  and effective_to = (
                    select max(effective_to) 
                    from demipt2.gold_dwh_dim_terminals_hist t1
                    where t.terminal_id = t1.terminal_id
                  )
                 """
        make_sql_query(conn=conn, query=query, logger=logger)

        # 5. Обновление информации об удалениях (для удаленных - флаг 'y')
        query = f"""
            update demipt2.gold_dwh_dim_terminals_hist
            set 
            effective_to = current_date - interval '1' second,
            deleted_flg = 'y'
            where terminal_id in (
            select
                t.terminal_id
            from demipt2.gold_dwh_dim_terminals_hist t
            left join demipt2.gold_stg_dim_terminals_del s
            on t.terminal_id = s.terminal_id
            where s.terminal_id is null
            )
            and deleted_flg = 'n'
            and effective_to = to_date( '9999-01-01', 'yyyy-mm-dd')
            """
        make_sql_query(conn=conn, query=query, logger=logger)

        # 6. Обновляем метаданные
        query = """
            update demipt2.gold_meta_bank
            set last_update_dt = ( select max(effective_from) from demipt2.gold_dwh_dim_terminals_hist )
            where table_db = 'demipt2' and table_name = 'gold_dwh_dim_terminals_hist' 
            and ( select max(effective_from) from demipt2.gold_dwh_dim_terminals_hist ) is not null
        """
        make_sql_query(conn=conn, query=query, logger=logger)

        # Переместим файл-источник в архив с маркировкой .backup
        source_path = os.path.join(path, filename)
        target_path = os.path.join(path, 'archive', f'{filename}.backup')

        rename_and_move_file(source_path=source_path, target_path=target_path, logger=logger)

