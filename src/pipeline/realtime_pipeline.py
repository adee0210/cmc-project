"""
Realtime Pipeline - Quản lý việc extract và load dữ liệu realtime liên tục.

Chạy liên tục mỗi 1 phút, không có sleep phức tạp.
"""

import asyncio
from datetime import datetime

from config.logger_config import LoggerConfig
from extract.realtime_extract import RealtimeExtract
from load.realtime_load import RealtimeLoad


class RealtimePipeline:
    """Pipeline để chạy realtime extract + load liên tục mỗi 1 phút."""

    def __init__(self):
        self.extractor = RealtimeExtract()
        self.loader = RealtimeLoad()
        self.logger = LoggerConfig.logger_config("Realtime Pipeline")
        self.is_running = False

    async def run_once(self):
        """Chạy pipeline 1 lần (extract + load). Không raise exception để crash."""
        self.logger.info(f"\nVÒNG LẶP - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        try:
            # Extract dữ liệu (sẽ tự động bù khoảng trống)
            data_map = await self.extractor.extract()

            # Load vào MongoDB
            try:
                self.loader.realtime_load(data_map=data_map)
            except Exception as e:
                self.logger.error(f"Lỗi khi load dữ liệu: {str(e)}")
                # Không raise, tiếp tục chạy vòng lặp tiếp theo
                return False

            # Thống kê
            total_records = sum(len(df) for df in data_map.values() if not df.empty)
            self.logger.info(f"HOÀN THÀNH - Tổng cộng: {total_records} bản ghi mới")

            return True

        except Exception as e:
            self.logger.error(f"Lỗi trong pipeline: {str(e)}")
            # Không raise, chỉ return False để tiếp tục vòng lặp
            return False

    async def run(self):
        """Chạy pipeline realtime liên tục mỗi 1 phút."""
        self.is_running = True
        self.logger.info("\nREALTIME PIPELINE - CHẠY LIÊN TỤC MỖI 1 PHÚT")
        self.logger.info("Nhấn Ctrl+C để dừng\n")

        run_count = 0

        try:
            while self.is_running:
                run_count += 1

                # Chạy pipeline, không để lỗi crash vòng lặp
                try:
                    success = await self.run_once()
                    if not success:
                        self.logger.warning("Vòng lặp gặp lỗi, sẽ thử lại sau 60 giây")
                except Exception as e:
                    self.logger.error(f"Lỗi không mong đợi trong run_once: {str(e)}")
                    # Không raise, tiếp tục vòng lặp

                # Chờ 60 giây trước khi chạy tiếp
                self.logger.info("Chờ 60 giây...\n")
                await asyncio.sleep(60)

        except KeyboardInterrupt:
            self.logger.info("\n\nNhận tín hiệu dừng (Ctrl+C)")
            self.logger.info(f"Tổng số vòng lặp đã chạy: {run_count}")
            self.logger.info("Đang dừng Realtime Pipeline")
            self.is_running = False
        except Exception as e:
            self.logger.error(f"\nLỗi nghiêm trọng trong vòng lặp chính: {str(e)}")
            # Log nhưng không raise, để pipeline có thể restart nếu cần
            self.is_running = False

    def stop(self):
        """Dừng pipeline."""
        self.is_running = False
        self.logger.info("Đã yêu cầu dừng pipeline")


if __name__ == "__main__":
    pipeline = RealtimePipeline()
    asyncio.run(pipeline.run())
