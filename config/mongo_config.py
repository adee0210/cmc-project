from pymongo import MongoClient
from config.variable_config import MONGO_CONFIG


class MongoConfig:
    _instance = None

    def _init_config(self):
        self._config = {
            "host": MONGO_CONFIG.get("host", "localhost"),
            "port": int(MONGO_CONFIG.get("port", 27017)),
            "username": MONGO_CONFIG.get("user"),
            "password": MONGO_CONFIG.get("pass"),
            "authSource": MONGO_CONFIG.get("authSource", "admin"),
        }

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(MongoConfig, cls).__new__(cls)
            cls._instance._init_config()
            cls._instance._client = None
        return cls._instance

    @property
    def get_config(self):
        return self._config

    def get_client(self):
        if self._client is None:
            self._client = MongoClient(**self._config)
        return self._client