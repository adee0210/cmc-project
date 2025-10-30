import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.convert_datetime_util import ConvertDatetime


def main():
    # Neu truyen doi so 'convert <iso_string>' thi in ket qua chuyen doi
    if len(sys.argv) >= 3 and sys.argv[1] == "convert":
        iso = sys.argv[2]
        conv = ConvertDatetime()
        print(conv.iso_to_sql_datetime(iso))
        return

    # Neu truyen doi so 'realtime' thi chay realtime pipeline LIEN TUC
    if len(sys.argv) >= 2 and sys.argv[1] == "realtime":
        from src.realtime_pipeline import RealtimePipeline

        print("\nChay Realtime Pipeline - LIEN TUC")

        # Lay interval tu argument neu co
        interval = 900  # mac dinh 15 phut
        if len(sys.argv) >= 3:
            try:
                interval = int(sys.argv[2])
                print(f"Interval: {interval} giay ({interval / 60:.1f} phut)")
            except ValueError:
                print(f"Interval khong hop le, dung mac dinh: {interval} giay")

        pipe = RealtimePipeline(loop_interval=interval)
        pipe.run(continuous=True)  # Chay lien tuc
        return

    # Neu truyen doi so 'all' thi chay historical TRUOC, sau do realtime LIEN TUC
    if len(sys.argv) >= 2 and sys.argv[1] == "all":
        from src.pipeline import HistoricalPipeline
        from src.realtime_pipeline import RealtimePipeline

        print("\n")
        print("BUOC 1: Chay Historical Pipeline - Lay du lieu lich su")
        historical_pipe = HistoricalPipeline()
        historical_pipe.run()

        print("\n")
        print("HOAN THANH Historical Pipeline")

        # Lay interval tu argument neu co
        interval = 900  # mac dinh 15 phut
        if len(sys.argv) >= 3:
            try:
                interval = int(sys.argv[2])
            except ValueError:
                pass

        print("\n")
        print("BUOC 2: Chay Realtime Pipeline - Che do LIEN TUC")
        print(f"Interval: {interval} giay ({interval / 60:.1f} phut)")

        realtime_pipe = RealtimePipeline(loop_interval=interval)
        realtime_pipe.run(continuous=True)  # Chay lien tuc
        return

    # Mac dinh: chay historical pipeline
    from src.pipeline import HistoricalPipeline

    print("\nChay Historical Pipeline (mac dinh)")
    pipe = HistoricalPipeline()
    pipe.run()


if __name__ == "__main__":
    main()
