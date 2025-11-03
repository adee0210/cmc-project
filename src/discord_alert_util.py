import requests
from datetime import datetime
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
                self.logger.info(f"Đã gửi cảnh báo Discord thành công")
                return True
            else:
                self.logger.error(
                    f"Lỗi gửi Discord alert. Status code: {response.status_code}"
                )
                return False

        except Exception as e:
            self.logger.error(f"Exception khi gửi Discord alert: {str(e)}")
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

        message = f"CẢNH BÁO: Không có dữ liệu từ {source}\n"
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
        message += f"Thời gian: {timestamp}\n"
        message += f"Lỗi: {error_message}\n"
        message += f"Vui lòng kiểm tra kết nối và cấu hình"

        self._send_discord_message(message)
