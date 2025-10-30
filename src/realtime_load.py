"""
Realtime Load - Load dữ liệu realtime vào MongoDB.
"""

from typing import Dict, Optional

import pandas as pd

from config.logger_config import LoggerConfig
from config.mongo_config import MongoConfig
from config.variable_config import EXTRACT_DATA_CONFIG


class RealtimeLoad:
    def __init__(self) -> None:
        try:
            self.logger = LoggerConfig.logger_config("Realtime Load CMC")
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
            self.logger.info("Ket noi MongoDB cho thao tac realtime load thanh cong")
        except Exception as e:
            try:
                self.logger.error(f"Khong the ket noi MongoDB: {str(e)}")
            except Exception:
                print(f"Loi khoi tao Mongo: {e}")
            raise

    def chunk_data_frame(self, realtime_data_extract: pd.DataFrame, chunk_size: int):
        for i in range(0, len(realtime_data_extract), chunk_size):
            yield realtime_data_extract.iloc[i : i + chunk_size]

    def realtime_load(
        self,
        realtime_data_extract: Optional[pd.DataFrame] = None,
        data_map: Optional[Dict[str, pd.DataFrame]] = None,
    ):
        """Load du lieu realtime vao MongoDB theo batch.

        Args:
            realtime_data_extract: DataFrame don le
            data_map: Dict mapping symbol -> DataFrame
        """
        if data_map is not None:
            for symbol, df in data_map.items():
                if df is None or df.empty:
                    self.logger.info(f"Khong co du lieu de load cho {symbol}")
                    continue
                self._load_dataframe(df, symbol)
            return

        if realtime_data_extract is not None:
            self._load_dataframe(realtime_data_extract)
            return

        self.logger.warning("Khong co du lieu duoc cung cap cho realtime_load")

    def _load_dataframe(self, df: pd.DataFrame, symbol: Optional[str] = None):
        self.logger.info(f"Bat dau load DataFrame cho {symbol or 'unknown symbol'}")
        chunk_size = int(self.batch_size_extract or 1000)
        batch_count = 0
        inserted_count = 0
        duplicate_count = 0

        for chunk in self.chunk_data_frame(df, chunk_size=chunk_size):
            try:
                chunk_data = chunk.to_dict("records")
                # Tao index tren truong datetime neu can
                try:
                    self.collection.create_index(
                        [("symbol", 1), ("datetime", 1)],
                        unique=True,
                        background=True,
                    )
                except Exception:
                    pass

                if chunk_data:
                    # Insert tung ban ghi, bo qua neu trung
                    for record in chunk_data:
                        try:
                            self.collection.insert_one(record)
                            inserted_count += 1
                        except Exception as e:
                            # Co the la duplicate key error
                            if "duplicate" in str(e).lower():
                                duplicate_count += 1
                            else:
                                self.logger.warning(f"Loi khi insert: {str(e)}")

                batch_count += 1
                self.logger.info(
                    f"Batch {batch_count} da xu ly: {len(chunk_data)} ban ghi"
                )
            except Exception as e:
                self.logger.error(f"Loi khi load du lieu realtime: {str(e)}")

        self.logger.info(
            f"Hoan thanh load - Inserted: {inserted_count}, Duplicate: {duplicate_count}, Tong batch: {batch_count}"
        )
