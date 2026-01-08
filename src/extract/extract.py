from datetime import datetime, timedelta
from typing import Dict, List
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests

from configs.logger_config import LoggerConfig
from configs.variable_config import EXTRACT_DATA_CONFIG
from util.convert_datetime_util import ConvertDatetime


class Extract:
    """Class Extract lấy dữ liệu lịch sử từ API CMC theo từng batch, lùi dần về quá khứ.

    Logic:
    - Bắt đầu từ ngày hiện tại, lùi dần về quá khứ theo batch_seconds
    - Tự động phát hiện API trả về bao nhiêu bản ghi trong mỗi batch
    - Tiếp tục lùi cho đến khi API không còn trả về dữ liệu
    - Hỗ trợ nhiều symbol, mỗi symbol có ID riêng
    """

    def __init__(self):
        self.logger = LoggerConfig.logger_config("Extract dữ liệu lịch sử CMC")
        self.config = EXTRACT_DATA_CONFIG
        self.api_config = self.config.get("api", {})
        self.url_template = self.api_config.get("url_template", "")
        self.interval = self.api_config.get("interval", "15m")
        self.batch_seconds = self.api_config.get("batch_seconds", 3 * 24 * 3600)
        self.convert_id = self.api_config.get("convert_id", 2781)
        self.symbols = self.config.get("symbols", ["eth"])
        self.cmc_symbol_ids = self.config.get("cmc_symbol_ids", {})
        self.converter = ConvertDatetime()

        # Delay giữa các request để tránh rate limit
        self.request_delay = 0.5  # seconds (giảm delay do xử lý song song)

        # Số lượng worker threads cho song song
        self.max_workers = self.api_config.get("max_workers", 5)

        self.logger.info(f"Khởi tạo Extract với symbols: {self.symbols}")
        self.logger.info(
            f"Batch seconds: {self.batch_seconds} ({self.batch_seconds // 86400} ngày)"
        )
        self.logger.info(f"Max workers (song song): {self.max_workers}")

    def extract(self) -> Dict[str, pd.DataFrame]:
        """Extract dữ liệu cho tất cả symbols SONG SONG.

        Returns:
            Dict mapping symbol -> DataFrame
        """
        result = {}

        self.logger.info(f"\n{'='*60}")
        self.logger.info(f"Bắt đầu extract SONG SONG cho {len(self.symbols)} symbols")
        self.logger.info(f"{'='*60}")

        # Sử dụng ThreadPoolExecutor để xử lý song song
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit tất cả các task
            future_to_symbol = {
                executor.submit(self.extract_symbol, symbol.lower()): symbol.lower()
                for symbol in self.symbols
            }

            # Xử lý kết quả khi hoàn thành
            for future in as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                try:
                    df = future.result()
                    result[symbol] = df
                    self.logger.info(
                        f"✓ Hoàn thành {symbol.upper()}: {len(df)} bản ghi"
                    )

                    # Gửi cảnh báo nếu không có data
                    if df.empty:
                        pass
                except Exception as e:
                    self.logger.error(f"✗ Lỗi khi extract {symbol.upper()}: {str(e)}")
                    result[symbol] = pd.DataFrame()

        self.logger.info(f"\n{'='*60}")
        self.logger.info(f"Hoàn thành extract toàn bộ symbols")
        self.logger.info(f"{'='*60}")

        return result

    def extract_symbol(self, symbol: str) -> pd.DataFrame:
        """Extract toàn bộ lịch sử cho một symbol, lùi dần về quá khứ.

        Args:
            symbol: Tên symbol (eth, bnb, xrp, ...)

        Returns:
            DataFrame chứa toàn bộ dữ liệu lịch sử
        """
        # Lấy CMC ID cho symbol
        cmc_id = self.cmc_symbol_ids.get(symbol.lower())
        if not cmc_id:
            self.logger.error(f"Không tìm thấy CMC ID cho symbol: {symbol}")
            return pd.DataFrame()

        all_data = []

        # Bắt đầu từ thời điểm hiện tại
        time_end = datetime.now()
        batch_count = 0
        total_records = 0

        self.logger.info(f"Bắt đầu từ thời điểm: {time_end}")

        while True:
            batch_count += 1

            # Tính time_start (lùi về quá khứ)
            time_start = time_end - timedelta(seconds=self.batch_seconds)

            self.logger.info(f"\nBatch #{batch_count}:")
            self.logger.info(f"  Từ: {time_start}")
            self.logger.info(f"  Đến: {time_end}")

            # Gọi API
            try:
                records = self._fetch_batch(
                    cmc_id=cmc_id, time_start=time_start, time_end=time_end
                )

                if not records or len(records) == 0:
                    self.logger.info(f"  ✓ Không còn dữ liệu - Dừng lại")
                    break

                self.logger.info(f"  ✓ Lấy được: {len(records)} bản ghi")
                all_data.extend(records)
                total_records += len(records)

                # Lùi thời gian cho batch tiếp theo
                time_end = time_start

                # Delay giữa các request
                time.sleep(self.request_delay)

            except Exception as e:
                self.logger.error(f"  ✗ Lỗi tại batch #{batch_count}: {str(e)}")
                # Có thể là đã hết dữ liệu hoặc lỗi API
                break

        self.logger.info(f"\n{'='*60}")
        self.logger.info(f"Tổng kết:")
        self.logger.info(f"  - Tổng số batch: {batch_count}")
        self.logger.info(f"  - Tổng số bản ghi: {total_records}")
        self.logger.info(f"{'='*60}")

        # Chuyển đổi thành DataFrame
        if not all_data:
            self.logger.warning(f"Không có dữ liệu nào được extract cho {symbol}")
            return pd.DataFrame()

        df = self._convert_to_dataframe(all_data, symbol)
        return df

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
        """Chuyển đổi list các bản ghi thành DataFrame chuẩn.

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
