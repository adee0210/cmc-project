"""
Realtime Load - Load dữ liệu realtime vào MongoDB.
"""

from typing import Dict, Optional

import pandas as pd

from configs.logger_config import LoggerConfig
from configs.mongo_config import MongoConfig
from configs.variable_config import EXTRACT_DATA_CONFIG


class RealtimeLoad:
    def __init__(self) -> None:
        self.logger = LoggerConfig.logger_config("Realtime Load CMC")
        self.batch_size_extract = EXTRACT_DATA_CONFIG.get("batch_size_extract", 1000)
        self.mongo_config = MongoConfig()
        # Không tạo client ngay, dùng lazy connection
        self.mongo_client = None
        self.db = None
        self.collection = None
        self.logger.info("Khởi tạo Realtime Load (lazy connection)")

    def _get_mongo_client(self):
        """Lazy connection: tạo client khi cần, tự động reconnect nếu bị đóng."""
        try:
            if self.mongo_client is None:
                self.logger.info("Đang kết nối MongoDB...")
                self.mongo_client = self.mongo_config.get_client()
                self.db = self.mongo_client.get_database(
                    EXTRACT_DATA_CONFIG.get("database", "cmc_db")
                )
                self.collection = self.db.get_collection(
                    EXTRACT_DATA_CONFIG.get("historical_collection", "cmc")
                )
                self.logger.info("Kết nối MongoDB thành công")
            return self.collection
        except Exception as e:
            self.logger.error(f"Lỗi kết nối MongoDB: {str(e)}")
            self.mongo_client = None
            self.db = None
            self.collection = None
            return None

    def chunk_data_frame(self, realtime_data_extract: pd.DataFrame, chunk_size: int):
        for i in range(0, len(realtime_data_extract), chunk_size):
            yield realtime_data_extract.iloc[i : i + chunk_size]

    def realtime_load(
        self,
        realtime_data_extract: Optional[pd.DataFrame] = None,
        data_map: Optional[Dict[str, pd.DataFrame]] = None,
    ):
        """Load dữ liệu realtime vào MongoDB theo batch.

        Args:
            realtime_data_extract: DataFrame đơn lẻ
            data_map: Dict mapping symbol -> DataFrame
        """
        if data_map is not None:
            for symbol, df in data_map.items():
                if df is None or df.empty:
                    self.logger.info(f"Không có dữ liệu để load cho {symbol}")
                    continue
                self._load_dataframe(df, symbol)
            return

        if realtime_data_extract is not None:
            self._load_dataframe(realtime_data_extract)
            return

        self.logger.warning("Không có dữ liệu được cung cấp cho realtime_load")

    def _load_dataframe(self, df: pd.DataFrame, symbol: Optional[str] = None):
        self.logger.info(f"Bắt đầu load DataFrame cho {symbol or 'unknown symbol'}")
        chunk_size = int(self.batch_size_extract or 1000)
        batch_count = 0
        inserted_count = 0
        duplicate_count = 0
        connection_errors = 0

        for chunk in self.chunk_data_frame(df, chunk_size=chunk_size):
            try:
                # Lấy collection với lazy connection
                collection = self._get_mongo_client()
                if collection is None:
                    self.logger.error("Không thể kết nối MongoDB, bỏ qua batch này")
                    continue

                chunk_data = chunk.to_dict("records")
                # Tạo index trên trường datetime nếu cần
                try:
                    collection.create_index(
                        [("symbol", 1), ("datetime", 1)],
                        unique=True,
                        background=True,
                    )
                except Exception:
                    pass

                if chunk_data:
                    # Insert từng bản ghi, bỏ qua nếu trùng
                    for record in chunk_data:
                        try:
                            collection.insert_one(record)
                            inserted_count += 1
                        except Exception as e:
                            error_str = str(e).lower()
                            # Kiểm tra lỗi duplicate
                            if "duplicate" in error_str:
                                duplicate_count += 1
                            # Kiểm tra lỗi connection
                            elif (
                                "closed" in error_str
                                or "connection" in error_str
                                or "timeout" in error_str
                            ):
                                connection_errors += 1
                                self.logger.warning(f"Lỗi kết nối MongoDB: {str(e)}")
                                # Đặt client về None để reconnect lần sau
                                self.mongo_client = None
                                self.db = None
                                self.collection = None
                                self.mongo_config.reset_client()
                                self.logger.info(
                                    "Đã đặt lại MongoDB client, sẽ reconnect lần tiếp theo"
                                )
                                break  # Thoát khỏi vòng lặp record, thử batch tiếp theo
                            else:
                                self.logger.warning(f"Lỗi khi insert: {str(e)}")

                batch_count += 1
                self.logger.info(
                    f"Batch {batch_count} đã xử lý: {len(chunk_data)} bản ghi"
                )
            except Exception as e:
                error_str = str(e).lower()
                if (
                    "closed" in error_str
                    or "connection" in error_str
                    or "timeout" in error_str
                ):
                    connection_errors += 1
                    self.logger.error(f"Lỗi kết nối MongoDB khi load batch: {str(e)}")
                    # Đặt lại client để reconnect
                    self.mongo_client = None
                    self.db = None
                    self.collection = None
                    self.mongo_config.reset_client()
                else:
                    self.logger.error(f"Lỗi khi load dữ liệu realtime: {str(e)}")

        self.logger.info(
            f"Hoàn thành load - Inserted: {inserted_count}, Duplicate: {duplicate_count}, Connection errors: {connection_errors}, Tổng batch: {batch_count}"
        )
