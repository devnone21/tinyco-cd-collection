"""
Logging service
"""
import os
import logging
from logging.handlers import TimedRotatingFileHandler
import logging_loki.emitter
from loguru import logger as loguru_logger
from dotenv import load_dotenv
load_dotenv()


def _get_file_handler(name: str = 'logfile') -> TimedRotatingFileHandler:
    formatter = logging.Formatter(
        '%(asctime)s | %(levelname)s - %(message)s'
    )
    file_handler = TimedRotatingFileHandler(os.getenv("LOG_PATH", f'{name}.log'),
                                            when='midnight',
                                            backupCount=2)
    file_handler.setFormatter(formatter)
    return file_handler


def _get_loki_handler() -> logging_loki.LokiHandler:
    logging_loki.emitter.LokiEmitter.level_tag = "level"
    loki_handler = logging_loki.LokiHandler(
        url="http://%s:%s/loki/api/v1/push" % (
            os.getenv("LOKI_HOST"),
            os.getenv("LOKI_PORT"),
        ),
        version="1",
    )
    return loki_handler


class Loggers:
    def __init__(self, **kwargs) -> None:
        self.name: str = kwargs.pop('name', "python")
        self.app: str = kwargs.pop('app', self.name)
        self.service: str = kwargs.pop('service', self.name)
        self.loguru = loguru_logger
        self.logging = logging.getLogger(name=self.name)
        self.logging.setLevel(logging.DEBUG)
        self.logging.addHandler(_get_file_handler(name=self.app))
        # self.logging.addHandler(_get_loki_handler())

    def _log(self, level=logging.DEBUG, message='') -> None:
        levelname: str = logging.getLevelName(level)
        extra = {"tags": {"service": self.service, "application": self.app}}
        # Loguru: default
        self.loguru.log(levelname, message)
        # Logging: [TimedRotatingFile, Loki]
        self.logging.log(level, message, extra=extra)

    def debug(self, message: str):
        return self._log(logging.DEBUG, message)

    def info(self, message: str):
        return self._log(logging.INFO, message)

    def warning(self, message: str):
        return self._log(logging.WARNING, message)

    def error(self, message: str):
        return self._log(logging.ERROR, message)

    def critical(self, message: str):
        return self._log(logging.CRITICAL, message)


logger = Loggers(app=os.getenv("APPLICATION", ""))

if __name__ == '__main__':
    logger.warning("New logger service!")
