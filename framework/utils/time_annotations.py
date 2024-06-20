import time
import os
from framework.utils.custlogging import LoggerProvider

logger = LoggerProvider().get_logger(os.path.basename(__file__))


class measure_execution_time_sec:

    def __init__(self, msg=None):
        self.msg = msg
        self.execution_time = None

    def __enter__(self):
        self.start_time = time.time()
        return self

    def __exit__(self, *args):
        self.execution_time = time.time() - self.start_time
        if self.msg is None:
            logger.info(f"Execution time: {self.execution_time} seconds")
        else:
            logger.info(f"{self.msg} : {self.execution_time} seconds")


class measure_execution_time_ms:

    def __init__(self, msg=None):
        self.msg = msg
        self.execution_time = None

    def __enter__(self):
        self.start_time = time.time()
        return self

    def __exit__(self, *args):
        self.execution_time = (time.time() - self.start_time) * 1000
        if self.msg is None:
            logger.debug(f"Execution time: {self.execution_time} ms")
        else:
            logger.debug(f"{self.msg} : {self.execution_time} ms")
