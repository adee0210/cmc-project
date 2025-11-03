"""
Realtime Pipeline - Quản lý việc extract và load dữ liệu realtime liên tục.

Pipeline này có 2 chức năng chính:
1. Bù dữ liệu thiếu (gap filling) - Chạy 1 lần khi khởi động
2. Cập nhật dữ liệu mới liên tục - Chạy theo interval
"""

import sys
import os
import time
import math
from datetime import datetime, timedelta

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config.logger_config import LoggerConfig
from src.realtime_extract import RealtimeExtract
from src.realtime_load import RealtimeLoad


class RealtimePipeline:
    """Pipeline để chạy realtime extract + load liên tục."""

    def __init__(self, loop_interval: int = 900):
        """
        Args:
            loop_interval: Thời gian nghỉ giữa các lần chạy (giây), mặc định 900s - 15 phút
        """
        self.extractor = RealtimeExtract()
        self.loader = RealtimeLoad()
        self.logger = LoggerConfig.logger_config("Realtime Pipeline")
        self.loop_interval = loop_interval
        self.is_running = False

    def run_once(self):
        """Chạy pipeline realtime một lần (bù dữ liệu + lấy mới nhất)."""
        self.logger.info("\nREALTIME PIPELINE - BẮT ĐẦU")

        try:
            # Extract dữ liệu (sẽ tự động bù khoảng trống)
            data_map = self.extractor.extract()

            # Load vào MongoDB
            self.loader.realtime_load(data_map=data_map)

            # Thống kê
            total_records = sum(len(df) for df in data_map.values() if not df.empty)
            self.logger.info(f"\nHOÀN THÀNH - Tổng cộng: {total_records} bản ghi mới")

            return True

        except Exception as e:
            self.logger.error(f"Lỗi trong pipeline: {str(e)}")
            return False

    def run(self, continuous: bool = False):
        """Chạy pipeline realtime.

        Args:
            continuous: Nếu True, chạy liên tục theo interval. Nếu False, chỉ chạy 1 lần.
        """
        if not continuous:
            # Chạy 1 lần
            self.run_once()
            return

        # Chạy liên tục
        self.is_running = True
        self.logger.info("\nREALTIME PIPELINE - CHẾ ĐỘ LIÊN TỤC")
        self.logger.info(
            f"Interval: {self.loop_interval} giây ({self.loop_interval / 60:.1f} phút)"
        )
        self.logger.info("Nhấn Ctrl+C để dừng\n")

        run_count = 0

        try:
            while self.is_running:
                run_count += 1
                self.logger.info(
                    f"\nVÒNG LẶP #{run_count} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                )

                success = self.run_once()

                if not success:
                    self.logger.warning("Pipeline gặp lỗi, sẽ thử lại sau")

                # Tính thời điểm chạy tiếp theo cần cân bằng với các mốc interval (vd: 00,15,30,45)
                now = datetime.now()
                try:
                    next_run_ts = (
                        math.ceil(now.timestamp() / self.loop_interval)
                        * self.loop_interval
                    )
                    next_run = datetime.fromtimestamp(next_run_ts)
                    sleep_seconds = (next_run - now).total_seconds()
                    # Nếu tính toán ra số âm (do runtime chậm hơn hợp lệ), fallback về loop_interval
                    if sleep_seconds <= 0:
                        sleep_seconds = self.loop_interval
                        next_run = now + timedelta(seconds=sleep_seconds)
                except Exception:
                    # fallback an toàn
                    next_run = now + timedelta(seconds=self.loop_interval)
                    sleep_seconds = self.loop_interval

                self.logger.info(
                    f"\nLần chạy tiếp theo: {next_run.strftime('%Y-%m-%d %H:%M:%S')}"
                )
                self.logger.info(
                    f"Nghỉ {int(sleep_seconds)} giây ({sleep_seconds / 60:.1f} phút)\n"
                )

                time.sleep(sleep_seconds)

        except KeyboardInterrupt:
            self.logger.info("\n\nNhận tín hiệu dừng (Ctrl+C)")
            self.logger.info(f"Tổng số vòng lặp đã chạy: {run_count}")
            self.logger.info("Đang dừng Realtime Pipeline")
            self.is_running = False
        except Exception as e:
            self.logger.error(f"\nLỗi nghiêm trọng: {str(e)}")
            self.is_running = False
            raise

    def stop(self):
        """Dừng pipeline."""
        self.is_running = False
        self.logger.info("Đã yêu cầu dừng pipeline")


if __name__ == "__main__":
    # Test realtime pipeline
    import argparse

    parser = argparse.ArgumentParser(description="Realtime Pipeline")
    parser.add_argument(
        "--continuous", action="store_true", help="Chạy liên tục theo interval"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=900,
        help="Thời gian nghỉ giữa các lần chạy (giây), mặc định 900s - 15 phút",
    )

    args = parser.parse_args()

    pipeline = RealtimePipeline(loop_interval=args.interval)
    pipeline.run(continuous=args.continuous)
