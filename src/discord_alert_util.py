import requests
from datetime import datetime, timedelta
from typing import Optional
from config.variable_config import DISCORD_CONFIG
from config.logger_config import LoggerConfig


class DiscordAlertUtil:
    """
    Class để gửi cảnh báo lỗi về Discord khi có vấn đề với data extraction.
    """

    def __init__(self):
        self.logger = LoggerConfig.logger_config("Discord Alert")
        self.webhook_url = DISCORD_CONFIG["webhook_url"]
        self.enabled = DISCORD_CONFIG["enabled"]

        # Tracking thời gian data thành công cuối cùng cho realtime
        self.last_successful_data_time = {}
        self.no_data_threshold = timedelta(
            minutes=15
        )  # Cảnh báo sau 15 phút không có data

        if not self.enabled:
            self.logger.info("Discord alerts are disabled")
        elif not self.webhook_url:
            self.logger.warning("Discord webhook URL not configured")
            self.enabled = False
        else:
            self.logger.info("Discord alerts are enabled")

    def _send_discord_message(self, message: str) -> bool:
        """
        Gửi message tới Discord webhook

        Args:
            message: Nội dung cảnh báo

        Returns:
            bool: True nếu gửi thành công
        """
        if not self.enabled:
            return False

        try:
            payload = {"content": message}

            response = requests.post(self.webhook_url, json=payload, timeout=10)

            if response.status_code == 204:
                self.logger.info(f"Da gui canh bao Discord thanh cong")
                return True
            else:
                self.logger.error(
                    f"Loi gui Discord alert. Status code: {response.status_code}"
                )
                return False

        except Exception as e:
            self.logger.error(f"Exception khi gui Discord alert: {str(e)}")
            return False

    def alert_no_data_from_source(
        self, source: str, error_details: Optional[str] = None
    ):
        """
        Cảnh báo khi không lấy được data từ nguồn

        Args:
            source: Tên nguồn data (VD: Historical Extract, Realtime Extract)
            error_details: Chi tiết lỗi nếu có
        """
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        message = f"CANH BAO: Không có dữ liệu từ {source}\n"
        message += f"Thời gian: {timestamp}\n"

        if error_details:
            message += f"Chi tiết: {error_details}\n"

        message += f"Hệ thống không lấy được dữ liệu mới từ nguồn {source}"

        self._send_discord_message(message)

    def alert_data_fetch_error(self, source: str, error_message: str):
        """
        Cảnh báo khi có lỗi khi lấy data

        Args:
            source: Tên nguồn data
            error_message: Nội dung lỗi
        """
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        message = f"LỖI: Không thể lấy dữ liệu từ {source}\n"
        message += f"Thoi gian: {timestamp}\n"
        message += f"Lỗi: {error_message}\n"
        message += f"Vui lòng kiểm tra kết nối và cấu hình"

        self._send_discord_message(message)

    def alert_no_new_data_realtime(
        self, symbols: list, last_data_time: Optional[datetime] = None
    ):
        """
        Cảnh báo khi realtime không có data mới sau 15 phút

        Args:
            symbols: Danh sách symbols bị ảnh hưởng
            last_data_time: Thời gian của data cuối cùng
        """
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        message = f"CẢNH BÁO: Realtime Extract - Không có dữ liệu mới\n"
        message += f"Thoi gian: {timestamp}\n"
        message += f"Symbols: {', '.join([s.upper() for s in symbols])}\n"

        if last_data_time:
            message += f"Dữ liệu cuối: {last_data_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
            time_diff = datetime.now() - last_data_time
            minutes = int(time_diff.total_seconds() / 60)
            message += f"Đã {minutes} phút không có dữ liệu mới\n"
        else:
            message += "Đã hơn 15 phút không có dữ liệu mới\n"

        message += "Vui lòng kiểm tra hệ thống"

        self._send_discord_message(message)

    def update_last_successful_data_time(self, source: str):
        """
        Cập nhật thời gian data thành công cuối cùng

        Args:
            source: Tên nguồn data
        """
        tracking_key = f"data_time_{source}"
        self.last_successful_data_time[tracking_key] = datetime.now()

    def check_and_alert_realtime_no_data(self, source: str, has_new_data: bool):
        """
        Kiểm tra và cảnh báo nếu realtime không có data mới sau 15 phút

        Args:
            source: Tên nguồn data
            has_new_data: True nếu có data mới
        """
        tracking_key = f"data_time_{source}"
        now = datetime.now()

        # Nếu có data mới, cập nhật thời gian
        if has_new_data:
            self.last_successful_data_time[tracking_key] = now
            return

        # Kiểm tra xem đã bao lâu không có data mới
        last_success = self.last_successful_data_time.get(tracking_key)

        if last_success:
            time_since_last_data = now - last_success

            # Nếu quá 15 phút không có data mới, gửi cảnh báo
            if time_since_last_data >= self.no_data_threshold:
                symbols = []  # Có thể thêm logic để track symbols
                self.alert_no_new_data_realtime(symbols, last_success)
        else:
            # Lần đầu tiên check, khởi tạo thời gian
            self.last_successful_data_time[tracking_key] = now
