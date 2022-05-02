# Загрузка данных о черном списке паспортов из Excel-файлов в DWH

import pandas as pd
import os
import re

from py_scripts.utils import rename_and_move_file, make_sql_query
from time import sleep
from datetime import datetime

# Функция для загрузки из Excel в Staging
def passport_blacklist_to_staging(conn, path, logger):
    """
    Ф-ция забирает файлы с данными о паспортах по заданному паттерну.
    Если файлов несколько, прогружает их последовательно, согласно дате в именах файлов по паттерну из задания.

    :param conn: коннектор к БД
    :param path: путь расположению файлов
    :param logger: логгер
    :return: None
    """

    # Создадим список всех файлов по паттерну terminals_DDMMYYYY.xlsx
    pattern = r'passport_blacklist_[0-9]+.\.xlsx'

    logger.info(f"""
    Start parsing data: passport_blacklist. Path: {path} Pattern: {pattern}
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
        sleep(pause_time)

        df = pd.read_excel(os.path.join(path, filename), converters={'date': str, 'passport': str})

        # Преобразуем даты к нужному для вставки формату
        list_to_insert = df.values.tolist()
        for i in list_to_insert:
            i[0] = i[0][:10]

        logger.info('Created dataframe:')
        logger.info(f'\n{df.to_string()}')

        # Запишем датафрейм в слой сырых данных demipt2.gold_stg_pssprt_blcklst_raw

        # Очистка предыдущей версии таблицы-источника в DWH
        query = """
                delete from demipt2.gold_stg_pssprt_blcklst_raw
                """
        make_sql_query(conn=conn, query=query, logger=logger)

        curs = conn.cursor()

        # Запись датафрейма в demipt2.gold_stg_pssprt_blcklst_raw
        try:
            query = f"""
                insert into demipt2.gold_stg_pssprt_blcklst_raw (
                        entry_dt,
                        passport_num
                    )
                values (to_date(?, 'yyyy-mm-dd'), ?)
                """
            logger.info('Insert data into demipt2.gold_stg_pssprt_blcklst_raw with query:\n')
            logger.info(query)
            curs.executemany(query, list_to_insert)
            logger.info('Data was inserted into demipt2.gold_stg_pssprt_blcklst_raw!')
        except Exception as e:
            logger.info(f'Data was not inserted into demipt2.gold_stg_pssprt_blcklst_raw! Exception: {e}')
            exit()

        conn.commit()
        curs.close()

        # Очистка стейджингов.
        query = """
                delete from demipt2.gold_stg_pssprt_blcklst
                """
        make_sql_query(conn=conn, query=query, logger=logger)

        # Захват данных в стейджинг (кроме удалений).
        query = """
                insert into demipt2.gold_stg_pssprt_blcklst ( passport_num, entry_dt )
                select passport_num, entry_dt 
                from demipt2.gold_stg_pssprt_blcklst_raw
                where entry_dt > ( 
                    select coalesce( last_update_dt, to_date( '1900-01-01', 'yyyy-mm-dd') ) 
                    from demipt2.gold_meta_bank 
                    where table_db = 'demipt2' and table_name = 'gold_dwh_fact_pssprt_blcklst' 
                    ) 
                """
        make_sql_query(conn=conn, query=query, logger=logger)

        # Делаем простую вставку изменений
        query = """
                insert into demipt2.gold_dwh_fact_pssprt_blcklst ( passport_num, entry_dt )
                select passport_num, entry_dt from demipt2.gold_stg_pssprt_blcklst
                """
        make_sql_query(conn=conn, query=query, logger=logger)

        # Обновляем метаданные.
        query = """
                update demipt2.gold_meta_bank
                set last_update_dt = ( select max(entry_dt) from demipt2.gold_stg_pssprt_blcklst )
                where table_db = 'demipt2' and table_name = 'gold_dwh_fact_pssprt_blcklst' 
                and ( select max(entry_dt) from demipt2.gold_stg_pssprt_blcklst ) is not null
                """
        make_sql_query(conn=conn, query=query, logger=logger)

        # Переместим файл-источник в архив с маркировкой .backup
        source_path = os.path.join(path, filename)
        target_path = os.path.join(path, 'archive', f'{filename}.backup')

        rename_and_move_file(source_path=source_path, target_path=target_path, logger=logger)
