# Выгрузка данных о терминалах из Excel-файла в стейджинг

import pandas as pd
import os
import re
from datetime import datetime

from py_scripts.utils import rename_and_move_file

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

    logger.info(f'List of files for processing: {filenames_list}')

    # Находим файл с наиболее свежей датой и выгружаем в датафрейм

    maxdate = datetime.min

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

    if maxdate.date() < date_from_metadata:
        logger.info(f'No actual data to insert! No dataframe will be created, exit function "terminals_to_staging"')
        return 0

    # Сформируем датафрейм
    df = pd.read_excel(os.path.join(path, filename_latest))

    logger.info('Created dataframe:')
    logger.info(f'\n{df.to_string()}')


    # Запишем датафрейм в стейджинговую таблицу GOLD_STG_DIM_TERMINALS
    # По условию задачи список терминалов - полносрезный, соответственно, инкрементальная загрузка не требуется
    # Поэтому каждый день перезаписываем стейджинговую таблицу GOLD_STG_DIM_TERMINALS

    # Очистка стейджинга
    try:
        curs.execute("""delete from demipt2.gold_stg_dim_terminals""")
        logger.info('Staging was cleared!')
    except Exception as e:
        logger.info(f'Staging was not cleared! Exception: {e}')

    # Захват данных в стейджинг (кроме удалений)
    try:
        curs.executemany(f"""
            insert into demipt2.GOLD_STG_DIM_TERMINALS (
                terminal_id,
                terminal_type,
                terminal_city,
                terminal_address,
                update_dt)
            values (?,?,?,?,to_date('{maxdate.date()}', 'yyyy-mm-dd')) 
            """, df.values.tolist())
        logger.info('Data was inserted!')
    except Exception as e:
        logger.info(f'Data was not inserted! Exception: {e}')

    curs.close()

    source_path = os.path.join(path, filename_latest)
    target_path = os.path.join(path, 'archive', filename_latest)

    rename_and_move_file(source_path=source_path, target_path=target_path, logger=logger)

    return 0






