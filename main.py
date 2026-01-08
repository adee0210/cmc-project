import os
import sys
import subprocess
import signal
import time
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

import asyncio
from datetime import datetime


class MainPipeline:
    def __init__(self):
        """Khởi tạo MainPipeline."""
        from configs.logger_config import LoggerConfig
        from src.pipeline.realtime_pipeline import RealtimePipeline

        self.logger = LoggerConfig.logger_config("MainPipeline", "main_pipeline.log")
        self.realtime_pipeline = RealtimePipeline()

    async def run_realtime(self):
        """Chạy realtime pipeline (async)."""
        try:
            self.logger.info("Khởi động pipeline thời gian thực")
            await self.realtime_pipeline.start()
        except Exception as e:
            self.logger.error(f"Lỗi pipeline thời gian thực: {e}")

    async def start(self):
        """Khởi động pipeline (async)."""
        try:
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.logger.info(f"Khởi động hệ thống tỷ giá lúc {current_time}")

            # Chạy realtime pipeline (async)
            self.logger.info("Khởi động pipeline thời gian thực...")
            await self.run_realtime()

            self.logger.info("Hệ thống tỷ giá hoàn thành")

        except KeyboardInterrupt:
            self.logger.info("Nhận tín hiệu dừng, tắt hệ thống")
        except Exception as e:
            self.logger.error(f"Lỗi hệ thống: {e}")


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
        print(f"Virtual environment chưa tồn tại. Đang tạo...")
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
    return get_project_root() / "exchange_rate_pipeline.pid"


def get_log_file():
    """Lấy đường dẫn log file"""
    log_dir = get_project_root() / "logs"
    log_dir.mkdir(exist_ok=True)
    return log_dir / "main_pipeline.log"


def run_pipelines():
    """Chạy pipeline chính."""
    try:
        print("Realtime pipeline: Chạy 9:00-9:45 sáng, mỗi 5 phút")
        print("=" * 70)

        pipeline = MainPipeline()
        asyncio.run(pipeline.start())
    except Exception as e:
        print(f"Lỗi khi chạy pipeline: {e}")
        import traceback

        traceback.print_exc()


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

        print(f"Đang dừng Exchange Rate Pipeline (PID: {pid})...")

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

        print("Exchange Rate Pipeline đã dừng thành công.")
        return True

    except Exception as e:
        print(f"Lỗi khi dừng: {e}")
        return False


def start_daemon():
    """Khởi động"""
    if is_daemon_running():
        pid = get_daemon_pid()
        print(f"Exchange Rate Pipeline đang chạy (PID: {pid})")
        return False

    python_exe = get_python_exe()
    project_root = get_project_root()
    script_path = project_root / "main.py"
    log_file = get_log_file()
    pid_file = get_pid_file()

    print("Đang khởi động Exchange Rate Pipeline...")
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
            print(f"Exchange Rate Pipeline đã khởi động (PID: {process.pid})")
            print(f"Dùng 'python main.py tail' để xem logs")
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
        print("Exchange Rate Pipeline không chạy")


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
    print("Đang khởi động lại Exchange Rate Pipeline...")
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


def main():
    """Entry point chính."""
    if len(sys.argv) < 2:
        show_help()
        return

    command = sys.argv[1].lower()

    if command == "--daemon":
        # Chạy logic chính
        run_pipelines()
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
        print(f"Lệnh không xác định: {command}")
        show_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
