import asyncio
import os
import sys
import signal
import logging
import subprocess
from pathlib import Path
from datetime import datetime


# Thêm thư mục gốc vào sys.path (để import config và src)
project_root = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(project_root, "src"))

# Tắt logging từ tvDatafeed để không spam console (nếu có)
logging.getLogger("tvDatafeed").setLevel(logging.CRITICAL)
logging.getLogger("tvDatafeed.main").setLevel(logging.CRITICAL)

from configs.logger_config import LoggerConfig
from pipeline.pipeline import HistoricalPipeline
from pipeline.realtime_pipeline import RealtimePipeline
from configs.mongo_config import MongoConfig
from configs.variable_config import EXTRACT_DATA_CONFIG
from util.convert_datetime_util import ConvertDatetime


def setup_venv_if_needed():
    """Tự động tạo virtual environment và cài đặt dependencies nếu chưa có."""
    venv_path = Path(".venv")
    requirements_file = Path("requirements.txt")

    if not venv_path.exists():
        print(" Tạo virtual environment...")
        subprocess.run([sys.executable, "-m", "venv", ".venv"], check=True)

        # Cài đặt dependencies
        if requirements_file.exists():
            print(" Cài đặt dependencies từ requirements.txt...")
            if os.name == "nt":  # Windows
                pip_exe = venv_path / "Scripts" / "pip.exe"
            else:  # Unix/Linux/Mac
                pip_exe = venv_path / "bin" / "pip"

            subprocess.run(
                [str(pip_exe), "install", "-r", "requirements.txt"], check=True
            )
            print(" Virtual environment đã được tạo và dependencies đã được cài đặt!")
        else:
            print(" Không tìm thấy requirements.txt")
    else:
        print(" Virtual environment đã tồn tại")


class CandlestickMain:
    """Main application class with resilient error handling and graceful shutdown"""

    def __init__(self, skip_existing=True):
        self.logger = LoggerConfig.logger_config("Main Candlestick")
        self.historical_completed = False
        self.skip_existing = skip_existing  # Chỉ trích xuất dữ liệu còn thiếu

        # Lấy cấu hình
        self.symbols = EXTRACT_DATA_CONFIG.get("symbols", ["eth", "bnb", "xrp"])
        self.database = EXTRACT_DATA_CONFIG.get("database", "cmc_db")
        self.collection_name = EXTRACT_DATA_CONFIG.get("historical_collection", "cmc")

        # Setup MongoDB connection
        self.mongo_config = MongoConfig()
        self.mongo_client = self.mongo_config.get_client()
        self.db = self.mongo_client.get_database(self.database)
        self.collection = self.db.get_collection(self.collection_name)

        # Setup signal handlers cho graceful shutdown
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
        self.shutdown_requested = False

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        self.logger.info(f"Nhận tín hiệu shutdown: {signum}")
        self.shutdown_requested = True

    def _check_if_historical_needed(self):
        """
        Kiểm tra xem có cần chạy historical extract không
        Logic: Chỉ chạy historical extract nếu CHƯA CÓ DATA hoặc có symbol chưa có data
        Returns: (needed, reason)
            - needed: True nếu cần chạy historical
            - reason: Lý do cần/không cần chạy
        """
        try:
            self.logger.info("=" * 80)
            self.logger.info("KIỂM TRA DỮ LIỆU TRONG DATABASE")
            self.logger.info("=" * 80)

            total_docs = self.collection.count_documents({})
            self.logger.info(f"Tổng số documents trong DB: {total_docs:,}")

            symbols_without_data = []
            symbols_with_data = []

            for symbol in self.symbols:
                # Đếm số lượng records của symbol
                count = self.collection.count_documents({"symbol": symbol.upper()})

                if count == 0:
                    self.logger.warning(
                        f"[{symbol.upper()}] Chưa có dữ liệu trong DB (0 records)"
                    )
                    symbols_without_data.append(symbol)
                else:
                    # Lấy thông tin data mới nhất
                    latest_record = self.collection.find_one(
                        {"symbol": symbol.upper()}, sort=[("datetime", -1)]
                    )
                    if latest_record and "datetime" in latest_record:
                        latest_dt_str = latest_record["datetime"]
                        self.logger.info(
                            f"[{symbol.upper()}] Đã có {count:,} records, mới nhất: {latest_dt_str}"
                        )
                    else:
                        self.logger.info(
                            f"[{symbol.upper()}] Đã có {count:,} records trong DB"
                        )
                    symbols_with_data.append(symbol)

            self.logger.info("=" * 80)
            if symbols_without_data:
                reason = (
                    f"Cần chạy historical cho {len(symbols_without_data)}/{len(self.symbols)} symbols chưa có data: "
                    f"{', '.join(symbols_without_data[:5])}"
                    + (
                        f" và {len(symbols_without_data) - 5} symbols khác"
                        if len(symbols_without_data) > 5
                        else ""
                    )
                )
                self.logger.info(f"KẾT LUẬN: {reason}")
                self.logger.info("SẼ CHẠY HISTORICAL EXTRACT")
                self.logger.info("=" * 80)
                return True, reason
            else:
                reason = (
                    f"Tất cả {len(self.symbols)} symbols đã có dữ liệu trong DB\n"
                    f"Realtime extract sẽ tự động cập nhật dữ liệu mới"
                )
                self.logger.info(f"KẾT LUẬN: {reason}")
                self.logger.info("BỎ QUA HISTORICAL EXTRACT - CHỈ CHẠY REALTIME")
                self.logger.info("=" * 80)
                return False, reason

        except Exception as e:
            self.logger.error(f"Lỗi khi kiểm tra data: {str(e)}")
            self.logger.exception(e)
            # Nếu lỗi, để an toàn thì KHÔNG chạy historical
            return False, "Lỗi khi kiểm tra → Bỏ qua historical để an toàn"

    def run_historical(self):
        """Chạy pipeline lịch sử (chỉ chạy 1 lần nếu cần) - với resilient error handling"""
        if self.historical_completed:
            self.logger.info("Pipeline lịch sử đã được chạy, bỏ qua")
            return

        # Kiểm tra xem có cần chạy historical extract không
        try:
            needed, reason = self._check_if_historical_needed()
        except Exception as e:
            self.logger.error(
                f"Lỗi khi kiểm tra historical data, bỏ qua historical: {str(e)}"
            )
            self.historical_completed = True
            return

        if not needed:
            self.logger.info(f"BỎ QUA HISTORICAL EXTRACT: {reason}")
            self.historical_completed = True
            return

        try:
            self.logger.info("=" * 80)
            self.logger.info("BẮT ĐẦU PIPELINE LỊCH SỬ")
            self.logger.info(f"Lý do: {reason}")
            if self.skip_existing:
                self.logger.info("Chế độ: Chỉ trích xuất dữ liệu còn thiếu")
            else:
                self.logger.info("Chế độ: Trích xuất toàn bộ lại")
            self.logger.info("=" * 80)

            historical_pipeline = HistoricalPipeline()
            # Chạy pipeline với error handling
            try:
                historical_pipeline.run()
            except Exception as e:
                self.logger.error(
                    f"Lỗi trong historical pipeline, nhưng sẽ tiếp tục realtime: {str(e)}"
                )

            self.historical_completed = True
            self.logger.info("=" * 80)
            self.logger.info("HOÀN THÀNH PIPELINE LỊCH SỬ")
            self.logger.info("=" * 80)
        except Exception as e:
            # Không raise, chỉ log để realtime vẫn chạy được
            self.logger.error(f"Lỗi khi chạy pipeline lịch sử: {str(e)}")
            self.logger.exception(e)
            self.historical_completed = True  # Đánh dấu hoàn thành để tiếp tục realtime

    def run_realtime(self):
        """Chạy pipeline realtime liên tục - với resilient error handling"""
        try:
            self.logger.info("=" * 80)
            self.logger.info("BẮT ĐẦU PIPELINE REALTIME")
            self.logger.info("=" * 80)

            realtime_pipeline = RealtimePipeline()

            # Bắt đầu vòng lặp realtime liên tục
            self.logger.info("=" * 80)
            self.logger.info("BẮT ĐẦU VÒNG LẶP REALTIME (CẬP NHẬT MỖI 1 PHÚT)")
            self.logger.info("=" * 80)

            # Chạy pipeline realtime (sẽ chạy liên tục bên trong)
            asyncio.run(realtime_pipeline.run())

            self.logger.info("=" * 80)
            self.logger.info("DỪNG PIPELINE REALTIME")
            self.logger.info("=" * 80)

        except Exception as e:
            self.logger.error(f"Lỗi nghiêm trọng khi chạy pipeline realtime: {str(e)}")
            self.logger.exception(e)

    def run(self):
        """Chạy toàn bộ ứng dụng"""
        try:
            self.logger.info("=" * 80)
            self.logger.info("KHỞI ĐỘNG ỨNG DỤNG CANDLESTICK")
            self.logger.info(f"Cấu hình: Async processing")
            if self.skip_existing:
                self.logger.info(
                    "Chế độ: Tự động kiểm tra và chỉ trích xuất dữ liệu còn thiếu"
                )
            else:
                self.logger.info("Chế độ: Trích xuất toàn bộ lại từ đầu")
            self.logger.info("=" * 80)

            # Bước 1: Chạy pipeline lịch sử (chỉ 1 lần)
            self.run_historical()

            # Bước 2: Chạy pipeline realtime liên tục
            self.run_realtime()

        except KeyboardInterrupt:
            self.logger.info("Nhận tín hiệu dừng từ người dùng - Thoát ứng dụng")
        except Exception as e:
            self.logger.error(f"Lỗi nghiêm trọng: {str(e)}")
            self.logger.exception(e)
            # Re-raise để trigger restart loop nếu cần
            raise
        finally:
            self.logger.info("=" * 80)
            self.logger.info("THOÁT ỨNG DỤNG CANDLESTICK")
            self.logger.info("=" * 80)


def main():
    # Tự động setup virtual environment nếu cần
    setup_venv_if_needed()

    # Nếu truyền đối số 'convert <iso_string>' thì in kết quả chuyển đổi
    if len(sys.argv) >= 3 and sys.argv[1] == "convert":
        iso = sys.argv[2]
        conv = ConvertDatetime()
        print(conv.iso_to_sql_datetime(iso))
        return

    # Nếu truyền đối số 'realtime' thì chỉ chạy realtime pipeline LIÊN TỤC
    if len(sys.argv) >= 2 and sys.argv[1] == "realtime":
        print("\n" + "=" * 80)
        print("CHẾ ĐỘ REALTIME - Chỉ chạy Realtime Pipeline")
        print("=" * 80)

        realtime_pipe = RealtimePipeline()
        asyncio.run(realtime_pipe.run())
        return

    # Nếu truyền đối số 'all' hoặc không có đối số → chạy ứng dụng đầy đủ
    if (len(sys.argv) >= 2 and sys.argv[1] == "all") or len(sys.argv) == 1:
        print("=" * 80)
        print("KHỞI ĐỘNG CANDLESTICK PIPELINE")
        print("=" * 80)
        print(f"Database: {EXTRACT_DATA_CONFIG.get('database', 'cmc_db')}")
        print(f"Collection: {EXTRACT_DATA_CONFIG.get('historical_collection', 'cmc')}")
        print(f"Interval: 1 phút")
        print(f"Symbols: {len(EXTRACT_DATA_CONFIG.get('symbols', []))} coins")
        print("=" * 80)
        print("Theo dõi log realtime: tail -f logs/main.log")
        print("=" * 80)
        print()

        try:
            main_app = CandlestickMain(skip_existing=True)
            main_app.run()
        except KeyboardInterrupt:
            print("\n✓ Dừng ứng dụng bởi người dùng (Ctrl+C)")
            logger = LoggerConfig.logger_config("Main Candlestick")
            logger.info("Ứng dụng dừng bởi người dùng")
        except Exception as e:
            print(f"\n✗ Lỗi: {str(e)}")
            logger = LoggerConfig.logger_config("Main Candlestick")
            logger.error(f"Ứng dụng bị crash: {str(e)}")
            logger.exception(e)
        return

    # Nếu truyền đối số 'historical' thì chỉ chạy historical pipeline
    if len(sys.argv) >= 2 and sys.argv[1] == "historical":
        print("\n" + "=" * 80)
        print("CHẾ ĐỘ HISTORICAL - Chỉ chạy Historical Pipeline")
        print("=" * 80)

        historical_pipe = HistoricalPipeline()
        historical_pipe.run()
        print("\n✓ Hoàn thành Historical Pipeline")
        return

    # Mặc định: hiển thị hướng dẫn sử dụng
    print("\nCách sử dụng:")
    print("  python main.py              # Chạy đầy đủ (historical + realtime)")
    print("  python main.py all          # Chạy đầy đủ (historical + realtime)")
    print("  python main.py realtime     # Chỉ chạy realtime")
    print("  python main.py historical   # Chỉ chạy historical")
    print("  python main.py convert ISO  # Convert ISO string")


if __name__ == "__main__":
    main()
