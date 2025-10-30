import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.convert_datetime_util import ConvertDatetime


def main():
    # Nếu truyền đối số 'convert <iso_string>' thì in kết quả chuyển đổi
    if len(sys.argv) >= 3 and sys.argv[1] == "convert":
        iso = sys.argv[2]
        conv = ConvertDatetime()
        print(conv.iso_to_sql_datetime(iso))
        return

    # Mặc định: chạy pipeline (extract -> load)
    from src.pipeline import HistoricalPipeline

    pipe = HistoricalPipeline()
    pipe.run()


if __name__ == "__main__":
    main()
