import os
import logging
import multiprocessing


def create_logger():
    '''
    Функция для создания логгера.

    :return: Logger object
    '''

    # Блок создания логгера
    logger = multiprocessing.get_logger()
    logger.setLevel(logging.INFO)

    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    sh = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    sh.setFormatter(formatter)
    logger.addHandler(sh)

    return logger