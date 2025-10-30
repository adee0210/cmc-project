"""
Test script để kiểm tra số lượng bản ghi mà API trả về với các khoảng thời gian khác nhau.
Mục đích: Phát hiện giới hạn số bản ghi của API và xác định khoảng thời gian tối ưu.
"""

import sys
import os
from datetime import datetime, timedelta
import requests
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), ".")))

from config.variable_config import EXTRACT_DATA_CONFIG


def test_api_with_timespan(symbol: str, cmc_id: int, days: int, interval: str = "15m"):
    """Test API với một khoảng thời gian cụ thể.

    Args:
        symbol: Tên symbol (ETH, BNB, XRP)
        cmc_id: CMC ID của symbol
        days: Số ngày để test
        interval: Khoảng thời gian giữa các điểm dữ liệu (15m, 1h, 1d, etc.)

    Returns:
        Số lượng bản ghi trả về
    """
    api_config = EXTRACT_DATA_CONFIG.get("api", {})
    url_template = api_config.get("url_template", "")
    convert_id = api_config.get("convert_id", 2781)

    # Tính toán thời gian
    time_end = datetime.now()
    time_start = time_end - timedelta(days=days)

    ts_start = int(time_start.timestamp())
    ts_end = int(time_end.timestamp())

    # Format URL
    url = url_template.format(
        id=cmc_id,
        convertId=convert_id,
        timeStart=ts_start,
        timeEnd=ts_end,
        interval=interval,
    )

    print(f"\n{'='*70}")
    print(f"Testing: {symbol.upper()}")
    print(f"Khoảng thời gian: {days} ngày(s)")
    print(f"Từ: {time_start.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Đến: {time_end.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Interval: {interval}")
    print(f"{'='*70}")

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        data = response.json()

        if "data" in data:
            quotes = data["data"].get("quotes", [])
            num_records = len(quotes)

            print(f"✓ Số bản ghi trả về: {num_records}")

            # Thông tin chi tiết
            if quotes:
                first_quote = quotes[0]
                last_quote = quotes[-1]

                first_time = first_quote.get("timeClose", "N/A")
                last_time = last_quote.get("timeClose", "N/A")

                print(f"  - Bản ghi đầu tiên: {first_time}")
                print(f"  - Bản ghi cuối cùng: {last_time}")

                # Tính toán số bản ghi lý thuyết
                if interval == "15m":
                    expected_records = days * 24 * 4  # 4 bản ghi mỗi giờ
                elif interval == "1h":
                    expected_records = days * 24
                elif interval == "1d":
                    expected_records = days
                else:
                    expected_records = None

                if expected_records:
                    print(f"  - Số bản ghi lý thuyết: {expected_records}")
                    print(
                        f"  - Tỷ lệ: {num_records}/{expected_records} = {num_records/expected_records*100:.2f}%"
                    )

                    if num_records < expected_records:
                        print(f"  ⚠️  API có thể đang giới hạn số bản ghi!")

            return num_records
        else:
            print("✗ Không có dữ liệu trong response")
            return 0

    except Exception as e:
        print(f"✗ Lỗi khi gọi API: {str(e)}")
        return 0


def test_cumulative_records(
    symbol: str, cmc_id: int, max_days: int = 7, interval: str = "15m"
):
    """Test xem số bản ghi có tăng tuyến tính khi tăng số ngày hay không.

    Nếu API có giới hạn, số bản ghi sẽ không tăng tuyến tính.
    """
    print(f"\n{'#'*70}")
    print(f"# TEST TÍCH LŨY: {symbol.upper()}")
    print(f"# Kiểm tra xem số bản ghi có tăng tuyến tính không")
    print(f"{'#'*70}")

    results = []

    for days in [1, 2, 3, 5, 7]:
        if days > max_days:
            break

        num_records = test_api_with_timespan(symbol, cmc_id, days, interval)
        results.append((days, num_records))

        # Delay để tránh rate limit
        time.sleep(1)

    # Phân tích kết quả
    print(f"\n{'='*70}")
    print(f"PHÂN TÍCH KẾT QUẢ CHO {symbol.upper()}:")
    print(f"{'='*70}")
    print(f"{'Số ngày':<15} {'Số bản ghi':<15} {'Tăng trưởng':<20}")
    print(f"{'-'*70}")

    for i, (days, records) in enumerate(results):
        if i == 0:
            growth = "-"
        else:
            prev_records = results[i - 1][1]
            if prev_records > 0:
                growth_rate = (records - prev_records) / prev_records * 100
                growth = f"+{records - prev_records} ({growth_rate:+.2f}%)"
            else:
                growth = "N/A"

        print(f"{days:<15} {records:<15} {growth:<20}")

    # Kết luận
    print(f"\n{'='*70}")
    print("KẾT LUẬN:")

    if len(results) >= 2:
        # Kiểm tra xem có giới hạn không
        first_day_records = results[0][1]
        last_day_records = results[-1][1]
        expected_ratio = results[-1][0] / results[0][0]
        actual_ratio = (
            last_day_records / first_day_records if first_day_records > 0 else 0
        )

        print(f"  - Tỷ lệ lý thuyết (ngày): {expected_ratio:.2f}x")
        print(f"  - Tỷ lệ thực tế (bản ghi): {actual_ratio:.2f}x")

        if abs(actual_ratio - expected_ratio) < 0.1:
            print(f"  ✓ Không có giới hạn rõ ràng - số bản ghi tăng tuyến tính")
        else:
            print(f"  ⚠️  Có thể có giới hạn - số bản ghi KHÔNG tăng tuyến tính")

            # Ước tính giới hạn
            max_records = max([r[1] for r in results])
            print(f"  ⚠️  Giới hạn ước tính: ~{max_records} bản ghi")

            # Đề xuất batch_seconds tối ưu
            if interval == "15m" and max_records > 0:
                # Mỗi giờ có 4 bản ghi (interval 15m)
                max_hours = max_records / 4
                max_days = max_hours / 24
                optimal_batch = int(max_days * 24 * 3600)

                print(
                    f"  💡 Đề xuất batch_seconds tối ưu: {optimal_batch} giây (~{max_days:.1f} ngày)"
                )

    print(f"{'='*70}\n")

    return results


def main():
    """Main function để chạy tất cả các test."""

    print("\n" + "=" * 70)
    print(" TEST API LIMIT - KIỂM TRA GIỚI HẠN SỐ BẢN GHI")
    print("=" * 70)

    symbols_config = EXTRACT_DATA_CONFIG.get("cmc_symbol_ids", {})
    interval = EXTRACT_DATA_CONFIG.get("api", {}).get("interval", "15m")

    print(f"\nCấu hình:")
    print(f"  - Interval: {interval}")
    print(f"  - Symbols: {list(symbols_config.keys())}")

    # Test cho từng symbol
    all_results = {}

    for symbol, cmc_id in symbols_config.items():
        results = test_cumulative_records(
            symbol=symbol, cmc_id=cmc_id, max_days=7, interval=interval
        )
        all_results[symbol] = results

        # Delay giữa các symbol
        time.sleep(2)

    # Tổng kết chung
    print(f"\n{'#'*70}")
    print("# TỔNG KẾT TOÀN BỘ SYMBOLS")
    print(f"{'#'*70}\n")

    for symbol, results in all_results.items():
        if results:
            max_records = max([r[1] for r in results])
            print(f"{symbol.upper():<10} - Số bản ghi tối đa: {max_records}")

    print("\n" + "=" * 70)
    print("Hoàn thành test!")
    print("=" * 70)


if __name__ == "__main__":
    main()
