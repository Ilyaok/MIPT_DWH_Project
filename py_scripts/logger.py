import os
import logging
import multiprocessing


def create_logger(path):
    '''
    Функция для создания логгера.

    Логгирование производится:
    1. Вывод сообщение на экран
    2. Запись в файл path/log_process.log

    :return: Logger object
    '''

    path_to_logs = os.path.join(path, "logs", "log_process.log")

    # Блок создания логгера
    logger = multiprocessing.get_logger()
    logger.setLevel(logging.INFO)

    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    sh = logging.StreamHandler()
    fh = logging.FileHandler(path_to_logs)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    sh.setFormatter(formatter)
    fh.setFormatter(formatter)
    logger.addHandler(sh)
    logger.addHandler(fh)

    return logger