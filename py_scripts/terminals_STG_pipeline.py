# Выгрузка данных о терминалах из Excel-файла в стейджинг

import pandas as pd
import os
import re

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

    files = [f for f in os.listdir(path) if re.match(r'terminals_[0-9]+.\.xlsx', f)]

    if len(files) == 0:
        logger.info(f'No files matching pattern detected. Pattern: {pattern}')

    print(files)

     # df = pd.read_excel()






