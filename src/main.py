import sys
from pathlib import Path
import os
import subprocess
import platform


def find_project_root(current_file, marker="requirements.txt"):
    current_path = Path(current_file).resolve()
    for parent in current_path.parents:
        if (parent / marker).exists():
            return parent
    return current_path.parent


project_root = find_project_root(__file__, marker="requirements.txt")
sys.path.insert(0, str(project_root))



def setup_environment():
    """Setup virtual environment and install packages if needed"""
    venv_path = project_root / ".venv"
    requirements_file = project_root / "requirements.txt"

    if not venv_path.exists():
        print("Virtual environment chưa tồn tại. Đang tạo...")
        try:
            subprocess.run([sys.executable, "-m", "venv", str(venv_path)], check=True)
            print("Virtual environment đã được tạo")
        except subprocess.CalledProcessError:
            print("Lỗi: Không thể tạo virtual environment")
            sys.exit(1)

    if platform.system() == "Windows":
        python_exe = venv_path / "Scripts" / "python.exe"
    else:
        python_exe = venv_path / "bin" / "python"

    if not python_exe.exists():
        print("Lỗi: Không tìm thấy Python executable trong venv")
        sys.exit(1)

    current_python = Path(sys.executable)
    venv_python = python_exe.resolve()
    if current_python != venv_python:
        print("Chuyển sang sử dụng virtual environment...")
        os.execv(str(venv_python), [str(venv_python)] + sys.argv)

    pip_exe = python_exe.parent / "pip.exe" if platform.system() == "Windows" else python_exe.parent / "pip"
    if requirements_file.exists():
        print("Cài đặt thư viện từ requirements.txt...")
        try:
            subprocess.run([str(pip_exe), "install", "-r", str(requirements_file)], check=True)
            print("Đã cài đặt xong các thư viện")
        except subprocess.CalledProcessError:
            print("Lỗi: Không thể cài đặt thư viện từ requirements.txt")
            sys.exit(1)


def run_in_background(args):
    pid_file = project_root / "cmc_project.pid"
    log_file = project_root / "logs" / "cmc_project.log"

    if pid_file.exists():
        print("CMC Project đã đang chạy")
        return

    print("Khởi động CMC Project trong background...")

    if platform.system() == "Windows":
        creationflags = subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        creationflags = 0

    process = subprocess.Popen(
        [sys.executable] + args,
        stdout=open(log_file, 'a'),
        stderr=subprocess.STDOUT,
        creationflags=creationflags,
        cwd=str(project_root)
    )

    # Save PID
    with open(pid_file, 'w') as f:
        f.write(str(process.pid))

    print(f"CMC Project đã khởi động (PID: {process.pid})")
    print(f"Log file: {log_file}")


def main():
    setup_environment()

    # Nếu truyền đối số 'stop' thì dừng process
    if len(sys.argv) >= 2 and sys.argv[1] == "stop":
        pid_file = project_root / "cmc_project.pid"
        if not pid_file.exists():
            print("Không tìm thấy file PID. Tiến trình có đang chạy không?")
            return
        pid = int(pid_file.read_text().strip())
        try:
            os.kill(pid, 9)
            pid_file.unlink()
            print("CMC Project đã dừng")
        except OSError:
            print("Tiến trình không chạy, xóa file PID cũ")
            pid_file.unlink()
        return

    # Nếu truyền đối số 'status' thì kiểm tra trạng thái
    if len(sys.argv) >= 2 and sys.argv[1] == "status":
        pid_file = project_root / "cmc_project.pid"
        log_file = project_root / "logs" / "cmc_project.log"
        if pid_file.exists():
            pid = int(pid_file.read_text().strip())
            try:
                os.kill(pid, 0)  # Check if process exists
                print(f"CMC Project đang chạy (PID: {pid})")
                print(f"Log file: {log_file}")
            except OSError:
                print("File PID tồn tại nhưng tiến trình không chạy")
        else:
            print("CMC Project không chạy")
        return

    if len(sys.argv) >= 2 and sys.argv[1] == "realtime":
        run_in_background(["src/main.py", "realtime_foreground"])
        return

    if len(sys.argv) >= 2 and sys.argv[1] == "realtime_foreground":
        import asyncio
        from pipeline.realtime_pipeline import RealtimePipeline

        print("\nChạy Realtime Pipeline - LIÊN TỤC MỖI 1 PHÚT")

        pipe = RealtimePipeline()
        asyncio.run(pipe.run())
        return

    if len(sys.argv) >= 2 and sys.argv[1] == "all":
        run_in_background(["src/main.py", "all_foreground"])
        return

    # Nếu truyền đối số 'all_foreground' (internal use)
    if len(sys.argv) >= 2 and sys.argv[1] == "all_foreground":
        from pipeline.pipeline import HistoricalPipeline
        from pipeline.realtime_pipeline import RealtimePipeline
        from config.mongo_config import MongoConfig
        from config.variable_config import EXTRACT_DATA_CONFIG

        print("\n")
        print("=" * 70)
        print("KIỂM TRA DỮ LIỆU LỊCH SỬ")
        print("=" * 70)

        # Kiểm tra xem đã có dữ liệu trong DB chưa
        mongo_config = MongoConfig()
        mongo_client = mongo_config.get_client()
        db = mongo_client.get_database(EXTRACT_DATA_CONFIG.get("database", "cmc_db"))
        collection = db.get_collection(
            EXTRACT_DATA_CONFIG.get("historical_collection", "cmc")
        )

        total_docs = collection.count_documents({})
        symbols = EXTRACT_DATA_CONFIG.get("symbols", ["eth", "bnb", "xrp"])

        print(f"Tổng số documents trong DB: {total_docs}")

        if total_docs > 0:
            # Kiểm tra chi tiết cho từng symbol
            print("\nThống kê theo symbol:")
            all_have_data = True
            for symbol in symbols:
                count = collection.count_documents({"symbol": symbol.upper()})
                print(f"  {symbol.upper()}: {count} bản ghi")
                if count == 0:
                    all_have_data = False

            if all_have_data:
                print("\n=> Đã có dữ liệu lịch sử cho tất cả symbols")
                print("=> BỎ QUA bước Historical Pipeline")
                print("\n" + "=" * 70)
            else:
                print("\n=> Có symbol chưa có dữ liệu")
                print("=> CHẠY Historical Pipeline")
                print("\n" + "=" * 70)
                print("BUỚC 1: Chạy Historical Pipeline - Lấy dữ liệu lịch sử")
                historical_pipe = HistoricalPipeline()
                historical_pipe.run()
                print("\nHOÀN THÀNH Historical Pipeline")
        else:
            print("\n=> Chưa có dữ liệu trong DB")
            print("=> CHAY Historical Pipeline")
            print("\n" + "=" * 70)
            print("BUOC 1: Chay Historical Pipeline - Lay du lieu lich su")
            historical_pipe = HistoricalPipeline()
            historical_pipe.run()
            print("\nHOAN THANH Historical Pipeline")

        print("\n")
        print("BUỚC 2: Chạy Realtime Pipeline - Chế độ LIÊN TỤC MỖI 1 PHÚT")

        import asyncio
        import time

        while True:
            try:
                realtime_pipe = RealtimePipeline()
                asyncio.run(realtime_pipe.run())
                break
            except Exception as e:
                print(f"\nLỗi nghiêm trọng trong Realtime Pipeline: {str(e)}")
                print("Pipeline sẽ được restart sau 30 giây...")
                time.sleep(30)
                print("Đang restart Realtime Pipeline...")
        return

    from pipeline.pipeline import HistoricalPipeline

    print("\nChạy Historical Pipeline (mặc định)")
    pipe = HistoricalPipeline()
    pipe.run()


if __name__ == "__main__":
    main()
