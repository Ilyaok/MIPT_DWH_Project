# Выгрузка данных о терминалах из Excel-файла в стейджинг

import pandas as pd
import os
import re
from datetime import datetime

from py_scripts.utils import rename_and_move_file, make_sql_query

# Функция для загрузки из Excel в Staging
def terminals_to_staging (conn, path, logger):
    '''
    В работе учтен случай, когда файлов может быть несколько
    (например, файлы не отправились в архив, или источник отбросил несколько файлов)
    Результат работы ф-ции - заполненная таблица GOLD_STG_DIM_TERMINALS

    :param conn: коннектор к БД
    :return: None
    '''

    # Создадим список всех файлов по паттерну terminals_DDMMYYYY.xlsx
    pattern = 'terminals_[0-9]+.\.xlsx'

    logger.info(f'Start parsing data about Terminals. Path: {path} Pattern: {pattern}')

    filenames_list = [f for f in os.listdir(path) if re.match(r'terminals_[0-9]+.\.xlsx', f)]

    if len(filenames_list) == 0:
        logger.info(f'No files matching pattern detected. Pattern: {pattern}')
        exit()

    logger.info(f'List of files for processing: {filenames_list}')

    # Находим файл с наиболее свежей датой и выгружаем в датафрейм

    maxdate = datetime.min
    filename_latest = ''

    for filename in filenames_list:
        date_time_str = filename[filename.find('_') + 1 : filename.find('.')]
        date_time_obj = datetime.strptime(date_time_str, '%d%m%Y')
        if date_time_obj > maxdate:
            maxdate = date_time_obj
            filename_latest = filename

    logger.info(f'The actual file: {filename_latest}')

    # Сверим maxdate с актуальной датой в таблице метаданных
    # Если maxdate меньше даты из demipt2.gold_meta_bank, значит, информация о терминалах актуальна на источнике
    # В этом случае не записываем новую информацию и отбрасываем файл filename_latest в архив

    curs = conn.cursor()

    curs.execute("""
        select coalesce( last_update_dt, to_date( '1900-01-01', 'YYYY-MM-DD') )
        from demipt2.gold_meta_bank where table_db = 'bank' and table_name = 'terminals'
    """)

    date_from_metadata = curs.fetchall()[0][0][:10]
    date_from_metadata = datetime.strptime(date_from_metadata, '%Y-%m-%d').date()

    logger.info(f'Actual date: {maxdate.date()}')
    logger.info(f'Date from metadata: {date_from_metadata}')

    if maxdate.date() <= date_from_metadata:
        logger.info(f'Maxdate <= date_from_metadata! No actual data to insert! No dataframe will be created, exit function "terminals_to_staging"')
        exit()

    # Сформируем датафрейм
    df = pd.read_excel(os.path.join(path, filename_latest))

    logger.info('Created dataframe:')
    logger.info(f'\n{df.to_string()}')

    # Запишем датафрейм в слой сырых данных demipt2.gold_stg_dim_terminals_raw
    # По условию задачи список терминалов - полносрезный, соответственно, инкрементальная загрузка не требуется
    # Поэтому каждый день перезаписываем таблицу demipt2.gold_stg_dim_terminals_raw

    query = """
            delete from demipt2.gold_stg_dim_terminals_raw
            """
    make_sql_query(conn=conn, query=query, logger=logger)

    try:
        curs.executemany(f"""
            insert into demipt2.gold_stg_dim_terminals_raw (
                terminal_id,
                terminal_type,
                terminal_city,
                terminal_address,
                update_dt)
            values (?,?,?,?,to_date('{maxdate.date()}', 'yyyy-mm-dd'))
            """, df.values.tolist())
        logger.info('Data was inserted into demipt2.gold_stg_dim_terminals_raw!')
    except Exception as e:
        logger.info(f'Data was not inserted into demipt2.gold_stg_dim_terminals_raw! Exception: {e}')

    conn.commit()

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
            terminal_id,
            terminal_type,
            terminal_city,
            terminal_address,
            update_dt
        from demipt2.gold_stg_dim_terminals_raw
        where update_dt > (
            select coalesce( last_update_dt, to_date( '1900-01-01', 'YYYY-MM-DD') )
            from demipt2.gold_meta_bank where table_db = 'bank' and table_name = 'terminals' )
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
    query = """
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
                where
                  t.terminal_id is null
                  or (
                  t.terminal_id is not null
                  and ( 1 = 0
                        or (s.terminal_type <> t.terminal_type) or (s.terminal_type is null and t.terminal_type is not null)
                        or (s.terminal_type is not null and t.terminal_type is null)
                      )
                  and ( 1 = 0
                        or (s.terminal_city <> t.terminal_city) or (s.terminal_city is null and t.terminal_city is not null)
                        or (s.terminal_city is not null and t.terminal_city is null)
                      )
                  and ( 1 = 0
                        or (s.terminal_address <> t.terminal_address) or (s.terminal_address is null and t.terminal_address is not null)
                        or (s.terminal_address is not null and t.terminal_address is null)
                      )
                  )
            ) stg
            on ( tgt.terminal_id = stg.terminal_id )
            when not matched then insert (
                terminal_id,
                terminal_type,
                terminal_city,
                terminal_address,
                deleted_flg,
                effective_from,
                effective_to
                )
            values (
                stg.terminal_id,
                stg.terminal_type,
                stg.terminal_city,
                stg.terminal_address,
                'n',
                stg.effective_from,
                to_date( '9999-01-01', 'yyyy-mm-dd')
                )
            when matched then
            update set effective_to = current_date - interval '1' second
            """
    make_sql_query(conn=conn, query=query, logger=logger)

    # 4.2 Вставка новой версии по scd2 для случая апдейта
    query = """
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
              or (
              t.terminal_id is not null
              and ( 1 = 0
                    or (s.terminal_type <> t.terminal_type) or (s.terminal_type is null and t.terminal_type is not null)
                    or (s.terminal_type is not null and t.terminal_type is null)
                  )
              and ( 1 = 0
                    or (s.terminal_city <> t.terminal_city) or (s.terminal_city is null and t.terminal_city is not null)
                    or (s.terminal_city is not null and t.terminal_city is null)
                  )
              and ( 1 = 0
                    or (s.terminal_address <> t.terminal_address) or (s.terminal_address is null and t.terminal_address is not null)
                    or (s.terminal_address is not null and t.terminal_address is null)
                  )
              )
            and effective_to <> to_date( '9999-01-01', 'yyyy-mm-dd')
                    """
    make_sql_query(conn=conn, query=query, logger=logger)

    # 5. Проставляем в приемнике флаг для удаленных записей ('y' - для удаленных)

    # 5.1 Вставляем актуальную запись по scd2
    query = """
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
            terminal_id,
            terminal_type,
            terminal_city,
            terminal_address,
            'y' as deleted_flg,
            current_date as effective_from,
            to_date( '9999-01-01', 'yyyy-mm-dd') as effective_to
        from demipt2.gold_dwh_dim_terminals_hist
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

    # 5.2 Обновляем данные об удаленной записи по scd2
    query = """
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
        where table_db = 'bank' and table_name = 'terminals' 
        and ( select max(effective_from) from demipt2.gold_dwh_dim_terminals_hist ) is not null
    """
    make_sql_query(conn=conn, query=query, logger=logger)

    # 7. Фиксируем транзакцию
    conn.commit()

    source_path = os.path.join(path, filename_latest)
    target_path = os.path.join(path, 'archive', filename_latest)

    rename_and_move_file(source_path=source_path, target_path=target_path, logger=logger)







