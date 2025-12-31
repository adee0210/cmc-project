from extract.extract import Extract as HistoricalExtract
from load.load import HistoricalLoad
from config.mongo_config import MongoConfig
from config.variable_config import EXTRACT_DATA_CONFIG
from config.logger_config import LoggerConfig


class HistoricalPipeline:
    def __init__(self):
        self.historical_extract = HistoricalExtract()
        self.historical_load = HistoricalLoad()
        self.logger = LoggerConfig.logger_config("Historical Pipeline")

    def check_historical_data_exists(self) -> bool:
        """Kiểm tra xem đã có đủ dữ liệu historical cho tất cả symbols chưa.

        Returns:
            bool: True nếu tất cả symbols đã có dữ liệu, False nếu cần extract
        """
        try:
            mongo_config = MongoConfig()
            mongo_client = mongo_config.get_client()
            db = mongo_client.get_database(
                EXTRACT_DATA_CONFIG.get("database", "cmc_db")
            )
            collection = db.get_collection(
                EXTRACT_DATA_CONFIG.get("historical_collection", "cmc")
            )

            symbols = EXTRACT_DATA_CONFIG.get("symbols", ["eth"])
            total_docs = collection.count_documents({})

            self.logger.info(f"Total documents in DB: {total_docs}")

            if total_docs == 0:
                self.logger.info("No historical data found - need to run extraction")
                return False

            # Check details for each symbol
            self.logger.info("Checking data for each symbol:")
            all_have_data = True
            for symbol in symbols:
                count = collection.count_documents({"symbol": symbol.upper()})
                self.logger.info(f"  {symbol.upper()}: {count} records")
                if count == 0:
                    all_have_data = False

            if all_have_data:
                self.logger.info(
                    "All symbols have historical data - skipping extraction"
                )
                return True
            else:
                self.logger.info("✗ Some symbols missing data - need to run extraction")
                return False

        except Exception as e:
            self.logger.error(f"Error checking historical data: {str(e)}")
            # Nếu không thể kiểm tra, cho phép chạy để đảm bảo dữ liệu
            return False

    def run(self):
        """Chạy historical pipeline với kiểm tra dữ liệu hiện có."""
        self.logger.info("=" * 60)
        self.logger.info("HISTORICAL PIPELINE START")
        self.logger.info("=" * 60)

        # Kiểm tra dữ liệu hiện có
        if self.check_historical_data_exists():
            self.logger.info("Skipping historical extraction - data already exists")
            self.logger.info("=" * 60)
            return

        # Extract dữ liệu
        self.logger.info("Starting historical data extraction...")
        historical_data = self.historical_extract.extract()

        # Load dữ liệu vào MongoDB
        self.logger.info("Loading historical data to MongoDB...")
        self.historical_load.historical_load(data_map=historical_data)

        self.logger.info("Historical pipeline completed")
        self.logger.info("=" * 60)


if __name__ == "__main__":
    historical_pipeline = HistoricalPipeline()
    historical_pipeline.run()
