import asyncio
import os
import sys
import signal
import logging
import subprocess
import time
from pathlib import Path
from datetime import datetime


# Thêm thư mục gốc vào sys.path (để import config và src)
project_root = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(project_root, "src"))

# Tắt logging từ tvDatafeed để không spam console (nếu có)
logging.getLogger("tvDatafeed").setLevel(logging.CRITICAL)
logging.getLogger("tvDatafeed.main").setLevel(logging.CRITICAL)

# Import modules sau khi đã setup sys.path
from configs.logger_config import LoggerConfig
from configs.mongo_config import MongoConfig
from configs.variable_config import EXTRACT_DATA_CONFIG
from util.convert_datetime_util import ConvertDatetime
from pipeline.pipeline import HistoricalPipeline
from pipeline.realtime_pipeline import RealtimePipeline


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


def get_project_root():
    """Lấy đường dẫn root của project"""
    return Path(__file__).parent.resolve()


def get_venv_path():
    """Lấy đường dẫn virtual environment"""
    return get_project_root() / ".venv"


def get_python_exe():
    """Lấy đường dẫn Python executable trong venv"""
    venv_path = get_venv_path()
    if not venv_path.exists():
        print("Virtual environment chưa tồn tại. Đang tạo...")
        try:
            # Tạo venv
            subprocess.run(
                [sys.executable, "-m", "venv", str(venv_path)],
                check=True,
                capture_output=True,
                text=True,
            )
            print(f"Đã tạo virtual environment tại {venv_path}")

            # Cài dependencies
            pip_exe = (
                venv_path / "bin" / "pip"
                if os.name != "nt"
                else venv_path / "Scripts" / "pip.exe"
            )
            requirements_file = get_project_root() / "requirements.txt"

            if requirements_file.exists():
                print("Đang cài đặt dependencies...")
                result = subprocess.run(
                    [str(pip_exe), "install", "-r", str(requirements_file)],
                    capture_output=True,
                    text=True,
                )
                if result.returncode == 0:
                    print("Đã cài đặt dependencies thành công")
                else:
                    print(f"Lỗi khi cài dependencies: {result.stderr}")
                    sys.exit(1)
        except subprocess.CalledProcessError as e:
            print(f"Lỗi khi tạo virtual environment: {e}")
            sys.exit(1)

    if os.name == "nt":
        return venv_path / "Scripts" / "python.exe"
    else:
        return venv_path / "bin" / "python"


def get_pid_file():
    """Lấy đường dẫn file PID."""
    return get_project_root() / "cmc_project.pid"


def get_log_file():
    """Lấy đường dẫn log file"""
    log_dir = get_project_root() / "logs"
    log_dir.mkdir(exist_ok=True)
    return log_dir / "main_pipeline.log"


def is_daemon_running():
    """Kiểm tra xem có đang chạy không"""
    pid_file = get_pid_file()
    if not pid_file.exists():
        return False

    try:
        with open(pid_file, "r") as f:
            pid = int(f.read().strip())

        # Kiểm tra process có tồn tại không (Linux)
        try:
            os.kill(pid, 0)  # Signal 0 để kiểm tra process tồn tại
            return True
        except OSError as e:
            # Chỉ xóa PID file nếu process THỰC SỰ không tồn tại (errno 3)
            if e.errno == 3:  # ESRCH - No such process
                pid_file.unlink()
            return False
    except (ValueError, FileNotFoundError):
        return False


def get_daemon_pid():
    """Lấy PID của daemon đang chạy"""
    pid_file = get_pid_file()
    if pid_file.exists():
        try:
            with open(pid_file, "r") as f:
                return int(f.read().strip())
        except (ValueError, FileNotFoundError):
            return None
    return None


def stop_daemon(force=False):
    """Dừng"""
    pid_file = get_pid_file()

    if not pid_file.exists():
        print("Không tìm thấy file PID. Daemon có thể chưa chạy.")
        return True

    try:
        with open(pid_file, "r") as f:
            pid = int(f.read().strip())

        print(f"Đang dừng CMC Pipeline (PID: {pid})...")

        try:
            os.kill(pid, signal.SIGTERM)

            # Đợi tối đa 10 giây
            for i in range(10):
                time.sleep(1)
                try:
                    os.kill(pid, 0)
                except OSError:
                    # Process đã dừng
                    break
            else:
                # Nếu sau 10 giây vẫn chạy, force kill
                if force:
                    print("Process vẫn đang chạy, force killing...")
                    os.kill(pid, signal.SIGKILL)
                    time.sleep(1)
        except OSError as e:
            if e.errno == 3:  # No such process
                print("Process đã dừng.")
            else:
                raise

        # Xóa PID file
        if pid_file.exists():
            pid_file.unlink()

        print("CMC Pipeline đã dừng thành công.")
        return True

    except Exception as e:
        print(f"Lỗi khi dừng: {e}")
        return False


def start_daemon():
    """Khởi động"""
    if is_daemon_running():
        pid = get_daemon_pid()
        print(f"CMC Pipeline đang chạy (PID: {pid})")
        return False

    python_exe = get_python_exe()
    project_root = get_project_root()
    script_path = project_root / "main.py"
    log_file = get_log_file()
    pid_file = get_pid_file()

    print("Đang khởi động CMC Pipeline...")
    print(f"Log file: {log_file}")

    try:
        with open(str(log_file), "a") as log_f:
            process = subprocess.Popen(
                [str(python_exe), str(script_path), "--daemon"],
                stdout=log_f,
                stderr=log_f,
                stdin=subprocess.DEVNULL,
                cwd=str(project_root),
                start_new_session=True,
            )

        # Lưu PID vào file
        with open(pid_file, "w") as f:
            f.write(str(process.pid))

        time.sleep(0.5)

        # Kiểm tra process có chạy không
        if process.poll() is None:
            print(f"CMC Pipeline đã khởi động (PID: {process.pid})")
            print("Dùng 'python main.py tail' để xem logs")
            return True
        else:
            print("Lỗi khi khởi động pipeline - process thoát ngay lập tức")
            if pid_file.exists():
                pid_file.unlink()
            return False

    except Exception as e:
        print(f"Lỗi khi khởi động: {e}")
        import traceback

        traceback.print_exc()
        return False


def show_status():
    """Hiển thị trạng thái"""
    if is_daemon_running():
        pid = get_daemon_pid()
        print(f"Exchange Rate Pipeline đang chạy (PID: {pid})")

        try:
            # Đọc thông tin từ /proc
            with open(f"/proc/{pid}/stat", "r") as f:
                stat = f.read().split()
                # stat[2] = state
                print(f"Trạng thái process: {stat[2]}")

            # Hiển thị uptime
            result = subprocess.run(
                ["ps", "-p", str(pid), "-o", "etime="], capture_output=True, text=True
            )
            if result.returncode == 0:
                print(f"Uptime: {result.stdout.strip()}")

        except Exception as e:
            print(f"Không thể lấy thông tin chi tiết: {e}")
    else:
        print("CMC Pipeline không chạy")


def tail_logs(lines=50):
    """Xem log"""
    log_file = get_log_file()
    if not log_file.exists():
        print(f"Không tìm thấy log file: {log_file}")
        return

    print(f"Hiển thị {lines} dòng cuối của {log_file}")
    print("=" * 80)
    subprocess.run(["tail", f"-n{lines}", str(log_file)])


def follow_logs():
    """Theo dõi log realtime"""
    log_file = get_log_file()
    if not log_file.exists():
        print(f"Không tìm thấy log file: {log_file}")
        return

    print(f"Theo dõi logs từ {log_file}")
    print("Nhấn Ctrl+C để dừng")
    print("=" * 80)
    try:
        subprocess.run(["tail", "-f", str(log_file)])
    except KeyboardInterrupt:
        print("\nĐã dừng theo dõi logs")


def restart_daemon():
    """Restart daemon"""
    print("Đang khởi động lại CMC Pipeline...")
    stop_daemon(force=True)
    time.sleep(2)
    start_daemon()


def show_help():
    """Hiển thị help"""
    print(
        """
Cách dùng: python main.py [command]

Lệnh:
    start       Khởi động pipeline
    stop        Dừng pipeline daemon
    restart     Khởi động lại pipeline daemon
    status      Hiển thị trạng thái daemon
    tail        Hiển thị 50 dòng log cuối
    logs        Theo dõi logs real-time
    help        Hiển thị hướng dẫn này

Ví dụ:
    python main.py start       # Khởi động
    python main.py status      # Kiểm tra trạng thái
    python main.py tail        # Xem log gần nhất
    python main.py logs        # Theo dõi logs
    python main.py restart     # Khởi động lại
    python main.py stop        # Dừng
"""
    )


class CandlestickMain:
    """Main application class with resilient error handling and graceful shutdown"""

    def __init__(self, skip_existing=True):
        from configs.logger_config import LoggerConfig
        from configs.mongo_config import MongoConfig
        from configs.variable_config import EXTRACT_DATA_CONFIG

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

            from pipeline.pipeline import HistoricalPipeline

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

            from pipeline.realtime_pipeline import RealtimePipeline

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
    """Entry point chính."""
    # Kiểm tra xem có đang chạy trong venv không
    if not hasattr(sys, "real_prefix") and not (
        hasattr(sys, "base_prefix") and sys.base_prefix != sys.prefix
    ):
        # Không phải venv, cần restart bằng venv python
        venv_python = get_python_exe()
        if str(venv_python) != sys.executable:
            # Restart với venv python
            args = [str(venv_python)] + sys.argv
            os.execv(str(venv_python), args)

    # Tự động setup virtual environment nếu cần
    setup_venv_if_needed()

    if len(sys.argv) < 2:
        # Không có tham số - chạy logic cũ của CandlestickMain
        from configs.variable_config import EXTRACT_DATA_CONFIG
        from configs.logger_config import LoggerConfig

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
            print("\nDừng ứng dụng bởi người dùng (Ctrl+C)")
            logger = LoggerConfig.logger_config("Main Candlestick")
            logger.info("Ứng dụng dừng bởi người dùng")
        except Exception as e:
            print(f"\nLỗi: {str(e)}")
            logger = LoggerConfig.logger_config("Main Candlestick")
            logger.error(f"Ứng dụng bị crash: {str(e)}")
            logger.exception(e)
        return

    command = sys.argv[1].lower()

    if command == "--daemon":
        # Chạy logic chính của CandlestickMain
        try:
            main_app = CandlestickMain(skip_existing=True)
            main_app.run()
        except Exception as e:
            print(f"Lỗi khi chạy daemon: {e}")
            import traceback

            traceback.print_exc()
    elif command == "start":
        start_daemon()
    elif command == "stop":
        stop_daemon(force=True)
    elif command == "restart":
        restart_daemon()
    elif command == "status":
        show_status()
    elif command in ["tail", "logs-tail"]:
        tail_logs()
    elif command in ["logs", "logs-follow"]:
        follow_logs()
    elif command == "help":
        show_help()
    else:
        # Fallback về logic cũ cho các commands như 'realtime', 'historical', 'convert', 'all'
        # Nếu truyền đối số 'convert <iso_string>' thì in kết quả chuyển đổi
        if len(sys.argv) >= 3 and sys.argv[1] == "convert":
            from util.convert_datetime_util import ConvertDatetime

            iso = sys.argv[2]
            conv = ConvertDatetime()
            print(conv.iso_to_sql_datetime(iso))
            return

        # Nếu truyền đối số 'realtime' thì chỉ chạy realtime pipeline LIÊN TỤC
        if len(sys.argv) >= 2 and sys.argv[1] == "realtime":
            from pipeline.realtime_pipeline import RealtimePipeline

            print("\n" + "=" * 80)
            print("CHẾ ĐỘ REALTIME - Chỉ chạy Realtime Pipeline")
            print("=" * 80)

            realtime_pipe = RealtimePipeline()
            asyncio.run(realtime_pipe.run())
            return

        # Nếu truyền đối số 'all' hoặc không có đối số → chạy ứng dụng đầy đủ
        if (len(sys.argv) >= 2 and sys.argv[1] == "all") or len(sys.argv) == 1:
            from configs.variable_config import EXTRACT_DATA_CONFIG
            from configs.logger_config import LoggerConfig

            print("=" * 80)
            print("KHỞI ĐỘNG CANDLESTICK PIPELINE")
            print("=" * 80)
            print(f"Database: {EXTRACT_DATA_CONFIG.get('database', 'cmc_db')}")
            print(
                f"Collection: {EXTRACT_DATA_CONFIG.get('historical_collection', 'cmc')}"
            )
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
                print("\nDừng ứng dụng bởi người dùng (Ctrl+C)")
                logger = LoggerConfig.logger_config("Main Candlestick")
                logger.info("Ứng dụng dừng bởi người dùng")
            except Exception as e:
                print(f"\nLỗi: {str(e)}")
                logger = LoggerConfig.logger_config("Main Candlestick")
                logger.error(f"Ứng dụng bị crash: {str(e)}")
                logger.exception(e)
            return

        # Nếu truyền đối số 'historical' thì chỉ chạy historical pipeline
        if len(sys.argv) >= 2 and sys.argv[1] == "historical":
            from pipeline.pipeline import HistoricalPipeline

            print("\n" + "=" * 80)
            print("CHẾ ĐỘ HISTORICAL - Chỉ chạy Historical Pipeline")
            print("=" * 80)

            historical_pipe = HistoricalPipeline()
            historical_pipe.run()
            print("\nHoàn thành Historical Pipeline")
            return

        # Mặc định: hiển thị hướng dẫn sử dụng
        print("\nCách sử dụng:")
        print("  python main.py              # Chạy đầy đủ (historical + realtime)")
        print("  python main.py all          # Chạy đầy đủ (historical + realtime)")
        print("  python main.py realtime     # Chỉ chạy realtime")
        print("  python main.py historical   # Chỉ chạy historical")
        print("  python main.py convert ISO  # Convert ISO string")
        print("  python main.py start        # Khởi động daemon")
        print("  python main.py stop         # Dừng daemon")
        print("  python main.py restart      # Khởi động lại daemon")
        print("  python main.py status       # Kiểm tra trạng thái daemon")
        print("  python main.py tail         # Xem log cuối")
        print("  python main.py logs         # Theo dõi logs")


if __name__ == "__main__":
    main()
