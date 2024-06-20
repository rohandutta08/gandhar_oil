import logging
import sys

from framework.utils.InProcessBatch import InProcessBatchManager
from framework.utils.singleton import SingletonMeta


class OneLineExceptionHandler(logging.Formatter):
    def format(self, record):
        if record.exc_info:
            record.msg = repr(super().formatException(record.exc_info))
            record.exc_info = None
            record.exc_text = None
        result = super().format(record)
        return result


FORMATTER = OneLineExceptionHandler(
    "%(levelname)s [%(filename)s:%(lineno)s]:  %(message)s Context: [ %(elt_client)s : %(batch_id)s] ")


class LoggerProvider(metaclass=SingletonMeta):

    def __init__(self):
        print("init logger")
        self.loggers = {}
        self.global_log_level = self.get_global_log_level("INFO")

    def get_logger(self, logger_name: str, log_level=None, add_filter: bool = True):
        log_level = log_level if log_level else self.global_log_level

        if self.loggers.get(logger_name):
            return self.loggers.get(logger_name)
        else:
            logger = logging.getLogger(logger_name)
            logger.setLevel(log_level)
            logger.addHandler(self.get_console_handler())
            if add_filter:
                logger.addFilter(LogFilter())
            else:
                logger.addFilter(LogFilterDummy())
            logger.propagate = False
            self.loggers[logger_name] = logger
            return logger

    @staticmethod
    def get_console_handler():
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(FORMATTER)
        return console_handler

    @staticmethod
    def get_global_log_level(level):
        level = level.upper() if level else ''
        log_levels = {
            'ERROR': logging.ERROR,
            'ERR': logging.ERROR,
            'WARNING': logging.WARNING,
            'WARN': logging.WARNING,
            'DEBUG': logging.DEBUG
        }
        return log_levels.get(level, logging.INFO)


class LogFilterDummy(logging.Filter):

    def __init__(self):
        super().__init__()

    def filter(self, record) -> bool:
        record.elt_client = None
        record.batch_id = None
        return True


class LogFilter(logging.Filter):

    def __init__(self):
        super().__init__()
        self.in_process_batch = InProcessBatchManager()

    def filter(self, record) -> bool:
        record.elt_client = self.in_process_batch.elt_client
        record.batch_id = self.in_process_batch.batch_id

        return True
