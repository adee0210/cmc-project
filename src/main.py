import os
import sys
import subprocess
import venv
from pathlib import Path


def setup_venv():
    """Tự động tạo virtual environment nếu chưa có và cài đặt dependencies."""
    venv_path = Path(".venv")
    if not venv_path.exists():
        print("Creating virtual environment...")
        venv.create(venv_path, with_pip=True)

    # Activate venv
    if os.name == "nt":  # Windows
        python_exe = venv_path / "Scripts" / "python.exe"
        pythonw_exe = venv_path / "Scripts" / "pythonw.exe"
        pip_exe = venv_path / "Scripts" / "pip.exe"
    else:
        python_exe = venv_path / "bin" / "python"
        pythonw_exe = None
        pip_exe = venv_path / "bin" / "pip"

    # Install requirements
    requirements_path = Path("requirements.txt")
    if requirements_path.exists():
        print("Installing requirements...")
        subprocess.check_call([str(pip_exe), "install", "-r", str(requirements_path)])

    return str(python_exe), str(pythonw_exe) if pythonw_exe else None


def run_pipelines():
    """Chạy historical pipeline nếu cần, sau đó realtime pipeline."""
    try:
        # Import sau khi venv được setup
        root_path = Path(__file__).parent.parent
        sys.path.insert(0, str(root_path))
        sys.path.insert(0, str(root_path / "src"))

        from pipeline.pipeline import HistoricalPipeline
        from pipeline.realtime_pipeline import RealtimePipeline
        import asyncio

        # Chạy historical pipeline nếu cần
        hist_pipeline = HistoricalPipeline()
        hist_pipeline.run()

        # Chạy realtime pipeline
        rt_pipeline = RealtimePipeline()
        asyncio.run(rt_pipeline.run())
    except Exception as e:
        print(f"Error in run_pipelines: {e}")
        import traceback

        traceback.print_exc()


def get_pid_file():
    return Path(__file__).parent.parent / "daemon.pid"


def is_daemon_running():
    pid_file = get_pid_file()
    if pid_file.exists():
        try:
            with open(pid_file, "r") as f:
                pid = int(f.read().strip())
            # Check if process exists
            import psutil

            return psutil.pid_exists(pid)
        except:
            return False
    return False


def kill_daemon():
    pid_file = get_pid_file()
    if pid_file.exists():
        try:
            with open(pid_file, "r") as f:
                pid = int(f.read().strip())
            if os.name == "nt":
                subprocess.run(["taskkill", "/PID", str(pid), "/F"], check=False)
            else:
                os.kill(pid, 9)
            pid_file.unlink()
            print("Daemon stopped.")
        except:
            print("Failed to stop daemon.")


def start_daemon():
    if is_daemon_running():
        print("Daemon is already running.")
        return

    python_exe, pythonw_exe = setup_venv()
    script_path = Path(__file__).resolve()

    if os.name == "nt":
        exe_to_use = pythonw_exe if pythonw_exe else python_exe
        proc = subprocess.Popen(
            [exe_to_use, str(script_path), "--daemon"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=subprocess.DETACHED_PROCESS,
        )
        with open(get_pid_file(), "w") as f:
            f.write(str(proc.pid))
        print("Daemon started in background.")
    else:
        print("Please run on Windows or implement Unix daemon.")


def main():
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        if command == "--daemon":
            # Chạy logic chính
            run_pipelines()
        elif command == "start":
            start_daemon()
        elif command == "stop":
            kill_daemon()
        elif command == "restart":
            kill_daemon()
            start_daemon()
        elif command == "status":
            if is_daemon_running():
                print("Daemon is running.")
            else:
                print("Daemon is not running.")
        else:
            print("Usage: python main.py [start|stop|restart|status]")
    else:
        # Default: start daemon
        start_daemon()


if __name__ == "__main__":
    main()
