"""
Realtime Extract - Lấy dữ liệu realtime và bù vào khoảng trống.

Logic:
1. Kiểm tra thời điểm cuối cùng có trong DB cho mỗi symbol
2. Lấy dữ liệu từ thời điểm đó đến hiện tại (bù khoảng trống)
3. Tự động cập nhật dữ liệu mới nhất
4. Chạy song song cho tất cả symbols bằng asyncio
"""

import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import pandas as pd
import requests
from pymongo import DESCENDING

from config.logger_config import LoggerConfig
from config.mongo_config import MongoConfig
from config.variable_config import EXTRACT_DATA_CONFIG
from util.convert_datetime_util import ConvertDatetime


class RealtimeExtract:
    def __init__(self):
        self.logger = LoggerConfig.logger_config("Realtime Extract")
        self.config = EXTRACT_DATA_CONFIG
        self.api_config = self.config.get("api", {})
        self.url_template = self.api_config.get("url_template", "")
        self.interval = self.api_config.get("interval", "15m")
        self.convert_id = self.api_config.get("convert_id", 2781)
        self.symbols = self.config.get("symbols", ["eth"])
        self.cmc_symbol_ids = self.config.get("cmc_symbol_ids", {})
        self.converter = ConvertDatetime()

        # Kết nối MongoDB để kiểm tra data
        self.mongo_config = MongoConfig()
        self.mongo_client = self.mongo_config.get_client()
        self.db = self.mongo_client.get_database(self.config.get("database", "cmc_db"))
        self.collection = self.db.get_collection(
            self.config.get("historical_collection", "cmc")
        )

        # API giới hạn 399 bản ghi, tương đương khoảng 4 ngày với interval 15m
        self.max_records_per_request = 399
        self.max_batch_seconds = 4 * 24 * 3600  # 4 ngay

        self.logger.info(f"Khởi tạo Realtime Extract với symbols: {self.symbols}")

    def get_latest_datetime_in_db(self, symbol: str) -> Optional[datetime]:
        """Lấy thời điểm mới nhất trong DB cho một symbol.

        Args:
            symbol: Tên symbol (eth, bnb, xrp)

        Returns:
            datetime của bản ghi mới nhất, hoặc None nếu chưa có dữ liệu
        """
        try:
            # Tìm bản ghi mới nhất theo datetime
            latest_record = self.collection.find_one(
                {"symbol": symbol.upper()}, sort=[("datetime", DESCENDING)]
            )

            if latest_record and "datetime" in latest_record:
                # Parse datetime string về datetime object
                datetime_str = latest_record["datetime"]
                latest_dt = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")
                self.logger.info(
                    f"Symbol {symbol.upper()}: Dữ liệu mới nhất trong DB: {latest_dt}"
                )
                return latest_dt
            else:
                self.logger.info(f"Symbol {symbol.upper()}: Chưa có dữ liệu trong DB")
                return None

        except Exception as e:
            self.logger.error(f"Lỗi khi lấy datetime mới nhất cho {symbol}: {str(e)}")
            return None

    async def extract(self) -> Dict[str, pd.DataFrame]:
        """Extract dữ liệu realtime cho tất cả symbols song song bằng asyncio.

        Returns:
            Dict mapping symbol -> DataFrame
        """
        self.logger.info("\nBẮT ĐẦU REALTIME EXTRACT")

        # Chạy song song tất cả symbols
        tasks = [self.extract_symbol_async(symbol.lower()) for symbol in self.symbols]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Xử lý kết quả
        result = {}
        for symbol, res in zip(self.symbols, results):
            symbol_lower = symbol.lower()
            if isinstance(res, Exception):
                self.logger.error(f"Lỗi khi extract {symbol_lower.upper()}: {str(res)}")
                result[symbol_lower] = pd.DataFrame()
            else:
                df, is_already_updated = res
                result[symbol_lower] = df

                if not df.empty:
                    self.logger.info(
                        f"{symbol_lower.upper()}: Lấy được {len(df)} bản ghi mới"
                    )
                else:
                    if is_already_updated:
                        self.logger.info(
                            f"{symbol_lower.upper()}: Không có dữ liệu mới (đã cập nhật)"
                        )
                    else:
                        self.logger.warning(
                            f"{symbol_lower.upper()}: Không lấy được dữ liệu từ API"
                        )

        self.logger.info("\nHOÀN THÀNH REALTIME EXTRACT")
        return result

    async def extract_symbol_async(self, symbol: str):
        """Async wrapper cho extract_symbol."""
        return await asyncio.to_thread(self.extract_symbol, symbol)

    def extract_symbol(self, symbol: str):
        """Extract dữ liệu realtime cho một symbol.

        Args:
            symbol: Tên symbol

        Returns:
            Tuple(DataFrame chứa dữ liệu mới, is_already_updated flag)
            - DataFrame: Dữ liệu mới từ API
            - bool: True nếu data đã cập nhật (không cần lấy thêm), False nếu có lỗi hoặc không có data từ API
        """
        # Lấy CMC ID
        cmc_id = self.cmc_symbol_ids.get(symbol.lower())
        if not cmc_id:
            self.logger.error(f"Không tìm thấy CMC ID cho symbol: {symbol}")
            return pd.DataFrame(), False

        # Lấy thời điểm mới nhất trong DB
        latest_dt = self.get_latest_datetime_in_db(symbol)

        # LOGIC ĐƠN GIẢN: Lấy từ DB_latest đến HIỆN TẠI
        # API sẽ tự trả về data có sẵn, không cần làm tròn phức tạp
        now = datetime.now()
        time_end = now

        self.logger.info(f"Thời điểm hiện tại: {now.strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info(f"Lấy dữ liệu đến: {time_end.strftime('%Y-%m-%d %H:%M:%S')}")

        if latest_dt:
            # Bắt đầu từ sau bản ghi mới nhất (thêm 1 phút để tránh trùng)
            time_start = latest_dt + timedelta(minutes=1)

            # Kiểm tra xem có cần lấy dữ liệu không
            time_diff = (time_end - time_start).total_seconds()

            if time_diff <= 0:
                self.logger.info(
                    f"Dữ liệu đã cập nhật (DB mới nhất: {latest_dt.strftime('%Y-%m-%d %H:%M:%S')})"
                )
                return pd.DataFrame(), True  # True = đã cập nhật, không cần cảnh báo

            self.logger.info(
                f"Khoảng trống cần bù: {time_diff / 60:.1f} phút (từ {time_start.strftime('%Y-%m-%d %H:%M')} đến {time_end.strftime('%Y-%m-%d %H:%M')})"
            )

        else:
            # Nếu chưa có dữ liệu, lấy 7 ngày gần nhất
            time_start = time_end - timedelta(days=7)
            self.logger.info("Chưa có dữ liệu trong DB, lấy 7 ngày gần nhất")

        # Nếu khoảng thời gian > max_batch_seconds, chia nhỏ ra
        all_data = []
        current_end = time_end

        while current_end > time_start:
            current_start = max(
                time_start, current_end - timedelta(seconds=self.max_batch_seconds)
            )

            self.logger.info(f"Lấy dữ liệu từ {current_start} đến {current_end}")

            try:
                records = self._fetch_batch(
                    cmc_id=cmc_id, time_start=current_start, time_end=current_end
                )

                if records:
                    self.logger.info(f"Lấy được: {len(records)} bản ghi")
                    all_data.extend(records)
                else:
                    self.logger.info(f"Không có dữ liệu trong batch này")

                # Lùi thời gian
                current_end = current_start

            except Exception as e:
                self.logger.error(f"Lỗi khi fetch batch: {str(e)}")
                break

        # Chuyển đổi thành DataFrame
        if not all_data:
            return pd.DataFrame(), False  # False = không có data từ API, cần cảnh báo

        df = self._convert_to_dataframe(all_data, symbol)

        # Loại bỏ các bản ghi đã có trong DB (dựa vào datetime)
        if latest_dt and not df.empty:
            latest_dt_str = latest_dt.strftime("%Y-%m-%d %H:%M:%S")
            original_len = len(df)
            df = df[df["datetime"] > latest_dt_str]
            removed = original_len - len(df)
            if removed > 0:
                self.logger.info(
                    f"Loại bỏ {removed} bản ghi trùng lặp (đã có trong DB)"
                )

        # Nếu sau khi loại bỏ trùng lặp mà không còn data
        if df.empty:
            if all_data:
                # Có data từ API nhưng tất cả đều trùng -> đã cập nhật, không cần cảnh báo
                self.logger.info("Tất cả dữ liệu từ API đều đã có trong DB")
                return df, True
            else:
                # Không có data từ API -> cần cảnh báo
                return df, False

        # Có data mới
        return df, False

    def _fetch_batch(
        self, cmc_id: int, time_start: datetime, time_end: datetime
    ) -> List[Dict]:
        """Gọi API để lấy dữ liệu trong một khoảng thời gian.

        Args:
            cmc_id: ID của coin trên CMC
            time_start: Thời điểm bắt đầu
            time_end: Thời điểm kết thúc

        Returns:
            List các bản ghi dạng dict
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

        self.logger.info(f"API URL: {url}")

        # Gọi API
        try:
            response = requests.get(url, timeout=30)
            self.logger.info(f"API Response Status: {response.status_code}")

            response.raise_for_status()

            data = response.json()
            self.logger.info(
                f"API Response Keys: {list(data.keys()) if isinstance(data, dict) else 'Not dict'}"
            )

            # Parse response
            if "data" not in data:
                self.logger.warning(f"API response không có key 'data': {data}")
                return []

            quotes = data["data"].get("quotes", [])
            self.logger.info(f"Số lượng records từ API: {len(quotes)}")

            if quotes:
                # Log sample record đầu tiên để debug
                sample = quotes[0]
                self.logger.info(
                    f"Sample record: timeClose={sample.get('timeClose')}, quote={sample.get('quote', {}).get('close')}"
                )

            return quotes

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Lỗi HTTP khi gọi API: {str(e)}")
            return []
        except Exception as e:
            self.logger.error(f"Lỗi khi parse response API: {str(e)}")
            return []

    def _convert_to_dataframe(self, records: List[Dict], symbol: str) -> pd.DataFrame:
        """Chuyển đổi list các bản ghi thành DataFrame.

        Args:
            records: List các quote từ API
            symbol: Tên symbol

        Returns:
            DataFrame đã được chuẩn hóa
        """
        rows = []

        for quote in records:
            try:
                # Lấy thông tin từ quote
                time_open = quote.get("timeOpen")
                time_close = quote.get("timeClose")
                time_high = quote.get("timeHigh")
                time_low = quote.get("timeLow")

                # Lấy thông tin giá
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
                self.logger.warning(f"Lỗi khi parse quote: {str(e)}")
                continue

        df = pd.DataFrame(rows)

        # Sắp xếp theo thời gian (tăng dần)
        if not df.empty and "datetime" in df.columns:
            df = df.sort_values("datetime").reset_index(drop=True)

        # Loại bỏ duplicate nếu có
        if not df.empty:
            df = df.drop_duplicates(subset=["symbol", "datetime"], keep="first")

        return df
