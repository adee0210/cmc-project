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
            "serverSelectionTimeoutMS": 5000,  # Timeout 5s
            "connectTimeoutMS": 10000,  # Timeout kết nối 10s
            "socketTimeoutMS": 10000,  # Timeout socket 10s
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

    def get_client(self, force_reconnect=False):
        """Lấy MongoDB client với lazy connection và auto-reconnect.

        Args:
            force_reconnect: Nếu True, đóng client cũ và tạo mới

        Returns:
            MongoClient instance
        """
        if force_reconnect and self._client is not None:
            try:
                self._client.close()
            except Exception:
                pass
            self._client = None

        if self._client is None:
            try:
                self._client = MongoClient(**self._config)
                # Test connection
                self._client.admin.command("ping")
            except Exception as e:
                print(f"Lỗi kết nối MongoDB: {e}")
                self._client = None
                raise
        return self._client

    def reset_client(self):
        """Đặt lại client về None để kích hoạt reconnect lần tiếp theo."""
        if self._client is not None:
            try:
                self._client.close()
            except Exception:
                pass
        self._client = None
