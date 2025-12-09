import sys
from pathlib import Path


def find_project_root(current_file, marker="requirements.txt"):
    current_path = Path(current_file).resolve()
    for parent in current_path.parents:
        if (parent / marker).exists():
            return parent
    return current_path.parent


project_root = find_project_root(__file__, marker="requirements.txt")
sys.path.insert(0, str(project_root))

from util.convert_datetime_util import ConvertDatetime


def main():
    # Nếu truyền đối số 'convert <iso_string>' thì in kết quả chuyển đổi
    if len(sys.argv) >= 3 and sys.argv[1] == "convert":
        iso = sys.argv[2]
        conv = ConvertDatetime()
        print(conv.iso_to_sql_datetime(iso))
        return

    # Nếu truyền đối số 'realtime' thì chạy realtime pipeline LIÊN TỤC
    if len(sys.argv) >= 2 and sys.argv[1] == "realtime":
        import asyncio
        from pipeline.realtime_pipeline import RealtimePipeline

        print("\nChạy Realtime Pipeline - LIÊN TỤC MỖI 1 PHÚT")

        pipe = RealtimePipeline()
        asyncio.run(pipe.run())
        return

    # Nếu truyền đối số 'all' thì chạy historical TRƯỚC, sau đó realtime LIÊN TỤC
    if len(sys.argv) >= 2 and sys.argv[1] == "all":
        from pipeline.pipeline import HistoricalPipeline
        from pipeline.realtime_pipeline import RealtimePipeline
        from config.mongo_config import MongoConfig
        from config.variable_config import EXTRACT_DATA_CONFIG

        print("\n")
        print("=" * 70)
        print("KIỂM TRA DỮ LIỆU LỊCH SỬ")
        print("=" * 70)

        # Kiểm tra xem đã có dữ liệu trong DB chưa
        mongo_config = MongoConfig()
        mongo_client = mongo_config.get_client()
        db = mongo_client.get_database(EXTRACT_DATA_CONFIG.get("database", "cmc_db"))
        collection = db.get_collection(
            EXTRACT_DATA_CONFIG.get("historical_collection", "cmc")
        )

        total_docs = collection.count_documents({})
        symbols = EXTRACT_DATA_CONFIG.get("symbols", ["eth", "bnb", "xrp"])

        print(f"Tổng số documents trong DB: {total_docs}")

        if total_docs > 0:
            # Kiểm tra chi tiết cho từng symbol
            print("\nThống kê theo symbol:")
            all_have_data = True
            for symbol in symbols:
                count = collection.count_documents({"symbol": symbol.upper()})
                print(f"  {symbol.upper()}: {count} bản ghi")
                if count == 0:
                    all_have_data = False

            if all_have_data:
                print("\n=> Đã có dữ liệu lịch sử cho tất cả symbols")
                print("=> BỎ QUA bước Historical Pipeline")
                print("\n" + "=" * 70)
            else:
                print("\n=> Có symbol chưa có dữ liệu")
                print("=> CHẠY Historical Pipeline")
                print("\n" + "=" * 70)
                print("BUỚC 1: Chạy Historical Pipeline - Lấy dữ liệu lịch sử")
                historical_pipe = HistoricalPipeline()
                historical_pipe.run()
                print("\nHOÀN THÀNH Historical Pipeline")
        else:
            print("\n=> Chưa có dữ liệu trong DB")
            print("=> CHAY Historical Pipeline")
            print("\n" + "=" * 70)
            print("BUOC 1: Chay Historical Pipeline - Lay du lieu lich su")
            historical_pipe = HistoricalPipeline()
            historical_pipe.run()
            print("\nHOAN THANH Historical Pipeline")

        print("\n")
        print("BUỚC 2: Chạy Realtime Pipeline - Chế độ LIÊN TỤC MỖI 1 PHÚT")

        import asyncio

        realtime_pipe = RealtimePipeline()
        asyncio.run(realtime_pipe.run())
        return

    from pipeline.pipeline import HistoricalPipeline

    print("\nChạy Historical Pipeline (mặc định)")
    pipe = HistoricalPipeline()
    pipe.run()


if __name__ == "__main__":
    main()
