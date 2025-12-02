import logging
import os
from logging.handlers import TimedRotatingFileHandler


class LoggerConfig:

    @staticmethod
    def logger_config(
        log_name: str, log_file: str = "cmc_project.log", log_level: int = logging.INFO
    ):
        # Lấy thư mục gốc của project (cmc-project/)
        # __file__ = /home/duc_le/cmc-project/config/logger_config.py
        # dirname(__file__) = /home/duc_le/cmc-project/config
        # dirname(dirname(__file__)) = /home/duc_le/cmc-project
        root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        base_path = os.path.join(root_dir, log_file)

        # formatter
        formatter = logging.Formatter(
            "%(asctime)s - %(processName)s - %(levelname)s - %(name)s - %(message)s"
        )

        # TimedRotatingFileHandler - Xoay vòng log mỗi ngày lúc nửa đêm
        # when='midnight' - Xoay vòng vào lúc 00:00:00 mỗi ngày
        # interval=1 - Mỗi 1 ngày
        # backupCount=7 - Giữ log của 7 ngày gần nhất
        # encoding='utf-8' - Hỗ trợ tiếng Việt
        # atTime=None - Xoay vòng đúng lúc nửa đêm
        file_handler = TimedRotatingFileHandler(
            filename=base_path,
            when="midnight",  # Xoay vòng lúc nửa đêm
            interval=1,  # Mỗi 1 ngày
            backupCount=3,  # Giữ 3 ngày
            encoding="utf-8",
            utc=False,  # Sử dụng local time
        )
        file_handler.setFormatter(formatter)
        # Đặt suffix cho file backup theo định dạng ngày
        file_handler.suffix = "%Y-%m-%d"

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)

        logger = logging.getLogger(log_name)

        if not logger.handlers:
            list_handler = [file_handler, console_handler]
            for h in list_handler:
                logger.addHandler(h)

        logger.propagate = False
        logger.setLevel(log_level)
        return logger
