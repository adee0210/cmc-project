from extract.extract import Extract as HistoricalExtract
from load.load import HistoricalLoad


class HistoricalPipeline:
    def __init__(self):
        self.historical_extract = HistoricalExtract()
        self.historical_load = HistoricalLoad()

    def run(self):
        """Chạy pipeline với streaming - extract và load theo batch để tránh tràn RAM"""
        # Extract và load theo từng symbol để tránh tràn RAM
        symbols = self.historical_extract.symbols

        for symbol in symbols:
            print(f"\n{'='*60}")
            print(f"Xử lý symbol: {symbol.upper()}")
            print(f"{'='*60}")

            # Extract từng symbol
            df = self.historical_extract.extract_symbol(symbol.lower())

            # Load ngay sau khi extract xong symbol này
            if not df.empty:
                self.historical_load._load_dataframe(df, symbol)
            else:
                print(f"Không có dữ liệu cho {symbol.upper()}")


if __name__ == "__main__":
    historical_pipeline = HistoricalPipeline()
    historical_pipeline.run()
