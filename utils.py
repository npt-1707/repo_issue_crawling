import logging
import json
import requests


def load_json(file_path):
    with open(file_path, "r") as f:
        return json.load(f)


def save_json(data, file_path):
    with open(file_path, "w") as f:
        json.dump(data, f, indent=4)


class Logger:
    def __init__(
        self,
        log_file,
        name=None,
        log_level=logging.INFO,
        console=True,
        file_mode="a",
        time=True,
    ):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(log_level)

        if time:
            formatter = logging.Formatter(
                "[%(asctime)s] - [%(levelname)s]: %(message)s"
            )
        else:
            formatter = logging.Formatter("[%(levelname)s]: %(message)s")

        # Create a file handler
        file_handler = logging.FileHandler(log_file, mode=file_mode)
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

        # Create a console handler
        if console:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(log_level)
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)

    def log(self, message, level=logging.INFO):
        self.logger.log(level, message)

    def info(self, message):
        self.logger.info(message)

    def warning(self, message):
        self.logger.warning(message)

    def error(self, message):
        self.logger.error(message)

    def critical(self, message):
        self.logger.critical(message)

    def debug(self, message):
        self.logger.debug(message)
