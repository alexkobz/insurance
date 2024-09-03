from os import getenv

import os
import requests
from dotenv import load_dotenv
from time import sleep
from typing import Dict

from rudata import DocsAPI
from logger.Logger import Logger


# init Logger
Logger()

class Token:
    """
    Class for getting token from RU DATA
    https://docs.efir-net.ru/dh2/#/Account/
    """

    _instance = None

    def __init__(self):
        if self._instance.__initialized:
            return
        self._instance.__initialized = True
        self.__url = getattr(DocsAPI, "Account").url
        self.__payload = getattr(DocsAPI, "Account")().payload()
        load_dotenv('.venv/.env')
        self.__payload["login"] = os.getenv("LOGIN")
        self.__payload["password"] = os.getenv("PASSWORD")
        # self.__payload["login"] = os.environ["LOGIN"]
        # self.__payload["password"] = os.environ["PASSWORD"]
        self._token_str: str = ""

    @staticmethod
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance: cls = object.__new__(cls, *args, **kwargs)
            cls._instance.__initialized = False
        return cls._instance

    @Logger.init_logger
    def __str__(self):
        if not self._token_str:
            response: Dict[str, str] = requests.post(self.__url, json=self.__payload).json()
            self._token_str: str = response["token"]
            sleep(1)
        return self._token_str

    @property
    def instance(self):
        return self._instance
