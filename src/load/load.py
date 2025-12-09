from typing import Dict, Optional

import pandas as pd

from config.logger_config import LoggerConfig
from config.mongo_config import MongoConfig
from config.variable_config import EXTRACT_DATA_CONFIG


class HistoricalLoad:
    def __init__(self) -> None:
        try:
            self.logger = LoggerConfig.logger_config("Load dữ liệu lịch sử CMC")
            self.batch_size_extract = EXTRACT_DATA_CONFIG.get(
                "batch_size_extract", 1000
            )
            self.mongo_config = MongoConfig()
            self.mongo_client = self.mongo_config.get_client()
            self.db = self.mongo_client.get_database(
                EXTRACT_DATA_CONFIG.get("database", "cmc_db")
            )
            self.collection = self.db.get_collection(
                EXTRACT_DATA_CONFIG.get("historical_collection", "cmc")
            )
            self.logger.info("Kết nối MongoDB cho thao tác load thành công")
        except Exception as e:
            # nếu logger chưa khởi tạo được
            try:
                self.logger.error(f"Không thể kết nối MongoDB: {str(e)}")
            except Exception:
                print(f"Lỗi khởi tạo Mongo: {e}")
            raise

    def chunk_data_frame(self, historical_data_extract: pd.DataFrame, chunk_size: int):
        for i in range(0, len(historical_data_extract), chunk_size):
            yield historical_data_extract.iloc[i : i + chunk_size]

    def historical_load(
        self,
        historical_data_extract: Optional[pd.DataFrame] = None,
        data_map: Optional[Dict[str, pd.DataFrame]] = None,
    ):
        """Load a single DataFrame or a dict mapping symbol->DataFrame into MongoDB in chunks.

        If both historical_data_extract and data_map are provided, data_map takes precedence.
        """
        if data_map is not None:
            for symbol, df in data_map.items():
                if df is None or df.empty:
                    self.logger.info(f"Không có dữ liệu để load cho {symbol}")
                    continue
                self._load_dataframe(df, symbol)
            return

        if historical_data_extract is not None:
            self._load_dataframe(historical_data_extract)
            return

        self.logger.warning("Không có dữ liệu được cung cấp cho historical_load")

    def _load_dataframe(self, df: pd.DataFrame, symbol: Optional[str] = None):
        self.logger.info(f"Bắt đầu load DataFrame cho {symbol or 'unknown symbol'} ...")
        chunk_size = int(self.batch_size_extract or 1000)
        batch_count = 0
        for chunk in self.chunk_data_frame(df, chunk_size=chunk_size):
            try:
                chunk_data = chunk.to_dict("records")
                # tạo index trên trường datetime nếu cần
                try:
                    self.collection.create_index(
                        [("datetime", 1)], unique=False, background=True
                    )
                except Exception:
                    pass
                if chunk_data:
                    self.collection.insert_many(chunk_data, ordered=False)
                batch_count += 1
                self.logger.info(
                    f"Batch {batch_count} đã xử lý: {len(chunk_data)} bản ghi"
                )
            except Exception as e:
                self.logger.error(f"Lỗi khi load dữ liệu lịch sử: {str(e)}")
        self.logger.info(f"Tổng số batch đã xử lý: {batch_count}")
