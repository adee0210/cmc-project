import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from src.extract import Extract as HistoricalExtract
from src.load import HistoricalLoad


class HistoricalPipeline:
    def __init__(self):
        self.historical_extract = HistoricalExtract()
        self.historical_load = HistoricalLoad()

    def run(self):
        # Extract dữ liệu
        historical_data = self.historical_extract.extract()
        # Load dữ liệu vào MongoDB
        self.historical_load.historical_load(data_map=historical_data)


if __name__ == "__main__":
    historical_pipeline = HistoricalPipeline()
    historical_pipeline.run()
