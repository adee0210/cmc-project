#!/bin/bash

# Script để quản lý CMC Symbol Project  
# Sử dụng: ./run.sh start|stop|restart|status

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="$SCRIPT_DIR/cmc_project.pid"
LOG_FILE="$SCRIPT_DIR/cmc_project.log"

PYTHON_EXE="$SCRIPT_DIR/.venv/bin/python"
# Nếu đang chạy trên Windows (venv sử dụng Scripts), chuyển lại đường dẫn
if [ -d "$SCRIPT_DIR/.venv/Scripts" ]; then
    PYTHON_EXE="$SCRIPT_DIR/.venv/Scripts/python.exe"
fi
PYTHON_CMD="$PYTHON_EXE $SCRIPT_DIR/src/main.py all"

setup_venv() {
    if [ ! -d "$SCRIPT_DIR/.venv" ]; then
        echo "Virtual environment chưa tồn tại. Đang tạo..."
        # Thử tạo venv bằng python3 hoặc python
        if command -v python3 >/dev/null 2>&1; then
            python3 -m venv "$SCRIPT_DIR/.venv"
        else
            python -m venv "$SCRIPT_DIR/.venv"
        fi
        if [ $? -ne 0 ]; then
            echo "Lỗi: Không thể tạo virtual environment"
            exit 1
        fi
        echo "Virtual environment đã được tạo"

        echo "Kích hoạt virtual environment và cài đặt thư viện..."
        # kích hoạt virtualenv (POSIX)
        if [ -f "$SCRIPT_DIR/.venv/bin/activate" ]; then
            source "$SCRIPT_DIR/.venv/bin/activate"
        elif [ -f "$SCRIPT_DIR/.venv/Scripts/activate" ]; then
            # Cygwin/git-bash trên Windows
            source "$SCRIPT_DIR/.venv/Scripts/activate"
        fi
        pip install --upgrade pip
        pip install -r "$SCRIPT_DIR/requirements.txt"
        if [ $? -ne 0 ]; then
            echo "Lỗi: Không thể cài đặt thư viện từ requirements.txt"
            exit 1
        fi
        echo "Đã cài đặt xong các thư viện"
    else
        echo "Virtual environment đã tồn tại, bỏ qua bước setup"
    fi
}

start() {
    # Kiểm tra và setup virtual environment nếu cần
    setup_venv

    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if ps -p "$PID" > /dev/null 2>&1; then
            echo "CMC Project đã đang chạy (PID: $PID)"
            return 1
        else
            echo "Xóa file PID cũ"
            rm "$PID_FILE"
        fi
    fi

    echo "Khởi động CMC Project..."
    echo "  - Chạy Historical Pipeline (lần đầu)"
    echo "  - Sau đó chạy Realtime Pipeline (liên tục mỗi 15 phút)"
    # Dùng nohup nếu có, fallback chạy bình thường
    if command -v nohup >/dev/null 2>&1; then
        nohup $PYTHON_CMD > "$LOG_FILE" 2>&1 &
    else
        $PYTHON_CMD > "$LOG_FILE" 2>&1 &
    fi
    echo $! > "$PID_FILE"
    echo "CMC Project đã khởi động (PID: $(cat "$PID_FILE"))"
    echo "Log file: $LOG_FILE"
    echo ""
    echo "Xem log realtime:"
    echo "  tail -f $LOG_FILE"
}

stop() {
    if [ ! -f "$PID_FILE" ]; then
        echo "Không tìm thấy file PID. Tiến trình có đang chạy không?"
        return 1
    fi

    PID=$(cat "$PID_FILE")
    if ps -p "$PID" > /dev/null 2>&1; then
        echo "Dừng CMC Project (PID: $PID)..."
        kill "$PID"
        sleep 2
        if ps -p "$PID" > /dev/null 2>&1; then
            echo "Tiến trình vẫn đang chạy, ép buộc dừng..."
            kill -9 "$PID"
        fi
        rm "$PID_FILE"
        echo "CMC Project đã dừng"
    else
        echo "Tiến trình không chạy, xóa file PID cũ"
        rm "$PID_FILE"
    fi
}

restart() {
    stop
    sleep 2
    start
}

status() {
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if ps -p "$PID" > /dev/null 2>&1; then
            echo "CMC Project đang chạy (PID: $PID)"
            echo "Log file: $LOG_FILE"
            echo ""
            echo "Xem log realtime:"
            echo "  tail -f $LOG_FILE"
        else
            echo "File PID tồn tại nhưng tiến trình không chạy"
        fi
    else
        echo "CMC Project không chạy"
    fi
}

case "$1" in
    start)
        start
        ;;
    stop)
        stop
        ;;
    restart)
        restart
        ;;
    status)
        status
        ;;
    *)
        echo "Sử dụng: $0 {start|stop|restart|status}"
        echo ""
        echo "CMC Symbol Project - Extract dữ liệu từ CoinMarketCap"
        echo ""
        echo "Commands:"
        echo "  start     - Khởi động service (historical + realtime liên tục)"
        echo "  stop      - Dừng service"
        echo "  restart   - Khởi động lại service"
        echo "  status    - Kiểm tra trạng thái"
        echo ""
        echo "Workflow:"
        echo "  1. Chạy Historical Pipeline - Lấy toàn bộ lịch sử (chạy 1 lần)"
        echo "  2. Chạy Realtime Pipeline - Cập nhật liên tục (mỗi 15 phút)"
        exit 1
        ;;
esac
