# Выгрузка данных о терминалах из Excel-файла в стейджинг

import pandas as pd
import os
import re
from datetime import datetime

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
    # Если

    df = pd.read_excel(os.path.join(path, filename_latest))

    logger.info('Created dataframe:')
    logger.info(f'\n{df.to_string()}')


    # Запишем датафрейм в стейджинговую таблицу GOLD_STG_DIM_TERMINALS
    # По условию задачи список терминалов - полносрезный, соответственно, инкрементальная загрузка не требуется
    # Поэтому каждый день перезаписываем стейджинговую таблицу GOLD_STG_DIM_TERMINALS

    curs = conn.cursor()

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






