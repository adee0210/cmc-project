"""
Realtime Extract - Lay du lieu realtime va bu vao khoang trong.

Logic:
1. Kiem tra thoi diem cuoi cung co trong DB cho moi symbol
2. Lay du lieu tu thoi diem do den hien tai (bu khoang trong)
3. Tu dong cap nhat du lieu moi nhat
"""

import sys
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import time

import pandas as pd
import requests
from pymongo import DESCENDING

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config.logger_config import LoggerConfig
from config.mongo_config import MongoConfig
from config.variable_config import EXTRACT_DATA_CONFIG
from src.convert_datetime_util import ConvertDatetime


class RealtimeExtract:
    """Class de extract du lieu realtime va bu vao khoang trong."""

    def __init__(self):
        self.logger = LoggerConfig.logger_config("Realtime Extract CMC")
        self.config = EXTRACT_DATA_CONFIG
        self.api_config = self.config.get("api", {})
        self.url_template = self.api_config.get("url_template", "")
        self.interval = self.api_config.get("interval", "15m")
        self.convert_id = self.api_config.get("convert_id", 2781)
        self.symbols = self.config.get("symbols", ["eth"])
        self.cmc_symbol_ids = self.config.get("cmc_symbol_ids", {})
        self.converter = ConvertDatetime()

        # Ket noi MongoDB de kiem tra data
        self.mongo_config = MongoConfig()
        self.mongo_client = self.mongo_config.get_client()
        self.db = self.mongo_client.get_database(self.config.get("database", "cmc_db"))
        self.collection = self.db.get_collection(
            self.config.get("historical_collection", "cmc")
        )

        # Delay giua requests
        self.request_delay = 0.5

        # API gioi han 399 ban ghi, tuong duong khoang 4 ngay voi interval 15m
        self.max_records_per_request = 399
        self.max_batch_seconds = 4 * 24 * 3600  # 4 ngay

        self.logger.info(f"Khoi tao Realtime Extract voi symbols: {self.symbols}")

    def get_latest_datetime_in_db(self, symbol: str) -> Optional[datetime]:
        """Lay thoi diem moi nhat trong DB cho mot symbol.

        Args:
            symbol: Ten symbol (eth, bnb, xrp)

        Returns:
            datetime cua ban ghi moi nhat, hoac None neu chua co du lieu
        """
        try:
            # Tim ban ghi moi nhat theo datetime
            latest_record = self.collection.find_one(
                {"symbol": symbol.upper()}, sort=[("datetime", DESCENDING)]
            )

            if latest_record and "datetime" in latest_record:
                # Parse datetime string ve datetime object
                datetime_str = latest_record["datetime"]
                latest_dt = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")
                self.logger.info(
                    f"Symbol {symbol.upper()}: Du lieu moi nhat trong DB: {latest_dt}"
                )
                return latest_dt
            else:
                self.logger.info(f"Symbol {symbol.upper()}: Chua co du lieu trong DB")
                return None

        except Exception as e:
            self.logger.error(f"Loi khi lay datetime moi nhat cho {symbol}: {str(e)}")
            return None

    def extract(self) -> Dict[str, pd.DataFrame]:
        """Extract du lieu realtime cho tat ca symbols.

        Returns:
            Dict mapping symbol -> DataFrame
        """
        result = {}

        self.logger.info("\nBAT DAU REALTIME EXTRACT")

        for symbol in self.symbols:
            symbol_lower = symbol.lower()
            self.logger.info(f"\nXu ly: {symbol_lower.upper()}")

            try:
                df = self.extract_symbol(symbol_lower)
                result[symbol_lower] = df

                if not df.empty:
                    self.logger.info(
                        f"{symbol_lower.upper()}: Lay duoc {len(df)} ban ghi moi"
                    )
                else:
                    self.logger.info(
                        f"{symbol_lower.upper()}: Khong co du lieu moi (da cap nhat)"
                    )

            except Exception as e:
                self.logger.error(f"Loi khi extract {symbol_lower.upper()}: {str(e)}")
                result[symbol_lower] = pd.DataFrame()

            # Delay giua cac symbol
            if symbol != self.symbols[-1]:
                time.sleep(self.request_delay)

        self.logger.info("\nHOAN THANH REALTIME EXTRACT")

        return result

    def extract_symbol(self, symbol: str) -> pd.DataFrame:
        """Extract du lieu realtime cho mot symbol.

        Args:
            symbol: Ten symbol

        Returns:
            DataFrame chua du lieu moi
        """
        # Lay CMC ID
        cmc_id = self.cmc_symbol_ids.get(symbol.lower())
        if not cmc_id:
            self.logger.error(f"Khong tim thay CMC ID cho symbol: {symbol}")
            return pd.DataFrame()

        # Lay thoi diem moi nhat trong DB
        latest_dt = self.get_latest_datetime_in_db(symbol)

        # Tinh toan khoang thoi gian can lay
        time_end = datetime.now()

        if latest_dt:
            # Bat dau tu sau ban ghi moi nhat (them 15 phut de tranh trung)
            time_start = latest_dt + timedelta(minutes=15)

            # Kiem tra xem co can lay du lieu khong
            time_diff = (time_end - time_start).total_seconds()

            if time_diff < 900:  # < 15 phut
                self.logger.info(f"Du lieu da cap nhat (chenh lech < 15 phut)")
                return pd.DataFrame()

            self.logger.info(f"Khoang trong can bu: {time_diff / 3600:.2f} gio")

        else:
            # Neu chua co du lieu, lay 7 ngay gan nhat
            time_start = time_end - timedelta(days=7)
            self.logger.info("Chua co du lieu trong DB, lay 7 ngay gan nhat")

        # Neu khoang thoi gian > max_batch_seconds, chia nho ra
        all_data = []
        current_end = time_end

        while current_end > time_start:
            current_start = max(
                time_start, current_end - timedelta(seconds=self.max_batch_seconds)
            )

            self.logger.info(f"Lay du lieu tu {current_start} den {current_end}")

            try:
                records = self._fetch_batch(
                    cmc_id=cmc_id, time_start=current_start, time_end=current_end
                )

                if records:
                    self.logger.info(f"Lay duoc: {len(records)} ban ghi")
                    all_data.extend(records)
                else:
                    self.logger.info(f"Khong co du lieu trong batch nay")

                # Lui thoi gian
                current_end = current_start

                # Delay
                if current_end > time_start:
                    time.sleep(self.request_delay)

            except Exception as e:
                self.logger.error(f"Loi khi fetch batch: {str(e)}")
                break

        # Chuyen doi thanh DataFrame
        if not all_data:
            return pd.DataFrame()

        df = self._convert_to_dataframe(all_data, symbol)

        # Loai bo cac ban ghi da co trong DB (dua vao datetime)
        if latest_dt and not df.empty:
            latest_dt_str = latest_dt.strftime("%Y-%m-%d %H:%M:%S")
            original_len = len(df)
            df = df[df["datetime"] > latest_dt_str]
            removed = original_len - len(df)
            if removed > 0:
                self.logger.info(f"Loai bo {removed} ban ghi trung lap")

        return df

    def _fetch_batch(
        self, cmc_id: int, time_start: datetime, time_end: datetime
    ) -> List[Dict]:
        """Goi API de lay du lieu trong mot khoang thoi gian.

        Args:
            cmc_id: ID cua coin tren CMC
            time_start: Thoi diem bat dau
            time_end: Thoi diem ket thuc

        Returns:
            List cac ban ghi dang dict
        """
        # Chuyển datetime sang Unix timestamp
        ts_start = int(time_start.timestamp())
        ts_end = int(time_end.timestamp())

        # Format URL
        url = self.url_template.format(
            id=cmc_id,
            convertId=self.convert_id,
            timeStart=ts_start,
            timeEnd=ts_end,
            interval=self.interval,
        )

        # Gọi API
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        data = response.json()

        # Parse response
        if "data" not in data:
            return []

        quotes = data["data"].get("quotes", [])
        return quotes

    def _convert_to_dataframe(self, records: List[Dict], symbol: str) -> pd.DataFrame:
        """Chuyen doi list cac ban ghi thanh DataFrame.

        Args:
            records: List cac quote tu API
            symbol: Ten symbol

        Returns:
            DataFrame da duoc chuan hoa
        """
        rows = []

        for quote in records:
            try:
                # Lay thong tin tu quote
                time_open = quote.get("timeOpen")
                time_close = quote.get("timeClose")
                time_high = quote.get("timeHigh")
                time_low = quote.get("timeLow")

                # Lay thong tin gia
                quote_data = quote.get("quote", {})

                row = {
                    "symbol": symbol.upper(),
                    "datetime": self.converter.iso_to_sql_datetime(time_close),
                    "time_open": self.converter.iso_to_sql_datetime(time_open),
                    "time_close": self.converter.iso_to_sql_datetime(time_close),
                    "time_high": self.converter.iso_to_sql_datetime(time_high),
                    "time_low": self.converter.iso_to_sql_datetime(time_low),
                    "open": quote_data.get("open"),
                    "high": quote_data.get("high"),
                    "low": quote_data.get("low"),
                    "close": quote_data.get("close"),
                    "volume": quote_data.get("volume"),
                    "market_cap": quote_data.get("marketCap"),
                    "circulating_supply": quote_data.get("circulatingSupply"),
                }
                rows.append(row)

            except Exception as e:
                self.logger.warning(f"Loi khi parse quote: {str(e)}")
                continue

        df = pd.DataFrame(rows)

        # Sap xep theo thoi gian (tang dan)
        if not df.empty and "datetime" in df.columns:
            df = df.sort_values("datetime").reset_index(drop=True)

        # Loai bo duplicate neu co
        if not df.empty:
            df = df.drop_duplicates(subset=["symbol", "datetime"], keep="first")

        return df
