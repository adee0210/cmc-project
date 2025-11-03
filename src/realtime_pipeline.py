"""
Realtime Pipeline - Quan ly viec extract va load du lieu realtime lien tuc.

Pipeline nay co 2 chuc nang chinh:
1. Bu du lieu thieu (gap filling) - Chay 1 lan khi khoi dong
2. Cap nhat du lieu moi lien tuc - Chay theo interval
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
from src.discord_alert_util import DiscordAlertUtil


class RealtimePipeline:
    """Pipeline de chay realtime extract + load lien tuc."""

    def __init__(self, loop_interval: int = 900):
        """
        Args:
            loop_interval: Thoi gian nghi giua cac lan chay (giay), mac dinh 900s - 15 phut
        """
        self.extractor = RealtimeExtract()
        self.loader = RealtimeLoad()
        self.logger = LoggerConfig.logger_config("Realtime Pipeline")
        self.discord_alert = DiscordAlertUtil()
        self.loop_interval = loop_interval
        self.is_running = False

    def run_once(self):
        """Chay pipeline realtime mot lan (bu du lieu + lay moi nhat)."""
        self.logger.info("\nREALTIME PIPELINE - BAT DAU")

        try:
            # Extract du lieu (se tu dong bu khoang trong)
            data_map = self.extractor.extract()

            # Load vao MongoDB
            self.loader.realtime_load(data_map=data_map)

            # Thong ke
            total_records = sum(len(df) for df in data_map.values() if not df.empty)
            self.logger.info(f"\nHOAN THANH - Tong cong: {total_records} ban ghi moi")

            # Kiểm tra và gửi cảnh báo nếu không có data
            if total_records == 0:
                # Không có data mới - kiểm tra xem đã quá 15 phút chưa
                self.discord_alert.check_and_alert_realtime_no_data(
                    "Realtime Extract", has_new_data=False
                )
            else:
                # Có data mới - cập nhật thời gian thành công
                self.discord_alert.update_last_successful_data_time("Realtime Extract")

            return True

        except Exception as e:
            self.logger.error(f"Loi trong pipeline: {str(e)}")
            # Gửi cảnh báo lỗi
            self.discord_alert.alert_data_fetch_error("Realtime Pipeline", str(e))
            return False

    def run(self, continuous: bool = False):
        """Chay pipeline realtime.

        Args:
            continuous: Neu True, chay lien tuc theo interval. Neu False, chi chay 1 lan.
        """
        if not continuous:
            # Chay 1 lan
            self.run_once()
            return

        # Chay lien tuc
        self.is_running = True
        self.logger.info("\nREALTIME PIPELINE - CHE DO LIEN TUC")
        self.logger.info(
            f"Interval: {self.loop_interval} giay ({self.loop_interval / 60:.1f} phut)"
        )
        self.logger.info("Nhan Ctrl+C de dung\n")

        run_count = 0

        try:
            while self.is_running:
                run_count += 1
                self.logger.info(
                    f"\nVONG LAP #{run_count} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                )

                success = self.run_once()

                if not success:
                    self.logger.warning("Pipeline gap loi, se thu lai sau")

                # Tinh thoi diem chay tiep theo can can bang voi cac muc interval (vd: 00,15,30,45)
                now = datetime.now()
                try:
                    next_run_ts = (
                        math.ceil(now.timestamp() / self.loop_interval)
                        * self.loop_interval
                    )
                    next_run = datetime.fromtimestamp(next_run_ts)
                    sleep_seconds = (next_run - now).total_seconds()
                    # Neu tinh toan ra so am (do runtime cham hon hop le), fallback ve loop_interval
                    if sleep_seconds <= 0:
                        sleep_seconds = self.loop_interval
                        next_run = now + timedelta(seconds=sleep_seconds)
                except Exception:
                    # fallback an toan
                    next_run = now + timedelta(seconds=self.loop_interval)
                    sleep_seconds = self.loop_interval

                self.logger.info(
                    f"\nLan chay tiep theo: {next_run.strftime('%Y-%m-%d %H:%M:%S')}"
                )
                self.logger.info(
                    f"Nghi {int(sleep_seconds)} giay ({sleep_seconds / 60:.1f} phut)\n"
                )

                time.sleep(sleep_seconds)

        except KeyboardInterrupt:
            self.logger.info("\n\nNhan tin hieu dung (Ctrl+C)")
            self.logger.info(f"Tong so vong lap da chay: {run_count}")
            self.logger.info("Dang dung Realtime Pipeline")
            self.is_running = False
        except Exception as e:
            self.logger.error(f"\nLoi nghiem trong: {str(e)}")
            self.is_running = False
            raise

    def stop(self):
        """Dung pipeline."""
        self.is_running = False
        self.logger.info("Da yeu cau dung pipeline")


if __name__ == "__main__":
    # Test realtime pipeline
    import argparse

    parser = argparse.ArgumentParser(description="Realtime Pipeline")
    parser.add_argument(
        "--continuous", action="store_true", help="Chay lien tuc theo interval"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=900,
        help="Thoi gian nghi giua cac lan chay (giay), mac dinh 900s - 15 phut",
    )

    args = parser.parse_args()

    pipeline = RealtimePipeline(loop_interval=args.interval)
    pipeline.run(continuous=args.continuous)
