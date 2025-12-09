# CMC Symbol Project

Project để extract dữ liệu lịch sử và realtime từ CoinMarketCap API và lưu vào MongoDB.

##  Tính năng

### 1. **Historical Extract** (Lấy dữ liệu lịch sử)
- Lùi dần từ hiện tại về quá khứ
- API giới hạn 399 bản ghi/request → Tự động chia batch 4 ngày
- Xử lý song song nhiều symbols (ETH, BNB, XRP)
- Dừng tự động khi hết dữ liệu

### 2. **Realtime Extract** (Cập nhật dữ liệu mới liên tục)
- Kiểm tra thời điểm mới nhất trong DB
- Tự động bù vào khoảng trống
- Tránh duplicate data
- Chạy liên tục mỗi 1 phút, không cần cấu hình interval
- Chạy song song tất cả symbols bằng asyncio (không dùng thread pool)

##  Cài đặt

```bash
# Clone repository
git clone https://github.com/adee0210/cmc-project.git
cd cmc-project

chmod +x run.sh

# Khi chạy run.sh start sẽ tự động cài đặt thư viện ở trong requerements.txt
./run.sh restart
```

##  Cấu hình

Tạo file `.env` với nội dung:

```env
MONGO_HOST=localhost
MONGO_PORT=27017
MONGO_USER=your_username
MONGO_PASS=your_password
MONGO_AUTH=admin
```

##  Cách sử dụng

### 1. Chạy lần đầu (Historical + Realtime liên tục)
```bash
# Cách 1: Dùng Python trực tiếp
python src/main.py all

# Cách 2: Dùng script (chạy background nền để chạy liên tục)
./run.sh start
```

**Quy trình:**
1.  Chạy Historical Pipeline → Lấy toàn bộ lịch sử
2.  Chạy Realtime Pipeline → Bù dữ liệu thiếu do thời gian chạy lịch sử
3.  Realtime tiếp tục chạy **LIÊN TỤC** mỗi 1 phút (tự động, không cần cấu hình interval)

### 2. Chạy chỉ Realtime (liên tục mỗi 1 phút)
```bash
# Cách 1: Python trực tiếp
python src/main.py realtime

# Cách 2: Script background
./run.sh start
```

### 3. Quản lý Service (dùng run.sh)

```bash
# Khởi động
./run.sh start

# Dừng service
./run.sh stop

# Khởi động lại
./run.sh restart

# Kiểm tra trạng thái
./run.sh status

# Xem log realtime
./run.sh logs
# hoặc
tail -f cmc_project.log
```

### 4. Test API Limit
```bash
python test_api_limit.py
```

## 📊 Cấu trúc dữ liệu

Dữ liệu được lưu vào MongoDB với cấu trúc:

```json
{
  "symbol": "ETH",
  "datetime": "2025-10-30 14:30:00",
  "time_open": "2025-10-30 14:15:00",
  "time_close": "2025-10-30 14:29:59",
  "time_high": "2025-10-30 14:20:00",
  "time_low": "2025-10-30 14:25:00",
  "open": 3526.73,
  "high": 3528.14,
  "low": 3521.06,
  "close": 3521.06,
  "volume": 42449312685.79,
  "market_cap": 425026391598.58,
  "circulating_supply": 120709702.92,
  "timestamp": "2025-10-30 14:29:59"
}
```

##  Cấu hình nâng cao

File `config/variable_config.py`:

```python
EXTRACT_DATA_CONFIG = {
  "symbols": ["eth", "bnb", "xrp"],  # Symbols cần extract
  "api": {
    "interval": "15m",              # Interval dữ liệu
    "batch_seconds": 4 * 24 * 3600, # 4 ngày (tối ưu cho 399 bản ghi)
    "convert_id": 2781,              # USD
  },
}
```
###  **Realtime Mode - Cách hoạt động mới:**

1. **Lần chạy đầu tiên:**
  - Kiểm tra thời điểm mới nhất trong DB: `2025-10-30 10:00:00`
  - Hiện tại: `2025-10-30 14:30:00`
  - → Lấy data từ `10:15:00` đến `14:30:00` (bù 4.5 giờ thiếu)

2. **Các lần tiếp theo:**
  - Mỗi 1 phút, pipeline sẽ tự động lấy data mới nhất từ thời điểm cuối cùng trong DB đến hiện tại
  - Chạy song song tất cả symbols bằng asyncio
  - Luôn đảm bảo không bị miss data

##  API Limit

**CMC API giới hạn: 399 bản ghi/request**

- Interval 15m: 399 records = 99.75 giờ ≈ 4.16 ngày
- Đã tối ưu: `batch_seconds = 4 ngày`

##  Cấu trúc thư mục

```
cmc_symbol_project/
├── config/
│   ├── logger_config.py      # Cấu hình logging
│   ├── mongo_config.py        # Kết nối MongoDB
│   └── variable_config.py     # Cấu hình chung
├── src/
│   ├── convert_datetime_util.py  # Utility chuyển đổi datetime
│   ├── extract.py                # Historical extract
│   ├── realtime_extract.py       # Realtime extract
│   ├── load.py                   # Load vào MongoDB
│   ├── pipeline.py               # Historical pipeline
│   └── main.py                   # Entry point
├── test_api_limit.py         # Test giới hạn API
├── requirements.txt
└── README.md
```

##  Troubleshooting

### Lỗi kết nối MongoDB
- Kiểm tra MongoDB đang chạy: `mongosh`
- Kiểm tra credentials trong `.env`

### API trả về lỗi 429 (Rate Limit)
- Tăng `request_delay` trong code
- Giảm `max_workers` xuống 3 hoặc 2

### Dữ liệu bị trùng
- Realtime pipeline tự động loại bỏ duplicate

- MongoDB index unique trên `(symbol, datetime)`

