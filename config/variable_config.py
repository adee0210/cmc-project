import os
from dotenv import load_dotenv

load_dotenv()

MONGO_CONFIG = {
    "port": os.getenv("MONGO_PORT", 27017),
    "host": os.getenv("MONGO_HOST", "localhost"),
    "user": os.getenv("MONGO_USER"),
    "pass": os.getenv("MONGO_PASS"),
    "authSource": os.getenv("MONGO_AUTH", "admin"),
}

EXTRACT_DATA_CONFIG = {
    "database": "cmc_db",
    "historical_collection": "cmc",
    "symbols": ["eth", "bnb", "xrp"],
    # Các cấu hình liên quan tới việc gọi API để extract dữ liệu
    "api": {
        # Template URL phải chứa các placeholder: {id}, {convertId}, {timeStart}, {timeEnd}, {interval}
        "url_template": (
            "https://api.coinmarketcap.com/data-api/v3.1/cryptocurrency/historical?id={id}&convertId={convertId}&timeStart={timeStart}&timeEnd={timeEnd}&interval={interval}"
        ),
        # interval mặc định
        "interval": "15m",
        # số giây cho mỗi lần request khi lùi về quá khứ
        # API giới hạn 399 bản ghi => 399 * 15 phút = 99.75 giờ ≈ 4.16 ngày
        # Dùng 4 ngày để an toàn (345600 giây)
        "batch_seconds": 4 * 24 * 3600,  # 4 ngày = 345600 giây
        # convertId mặc định (cần chỉnh nếu muốn)
        "convert_id": 2781,
        # số lượng worker threads cho xử lý song song (mặc định 5)
        "max_workers": 5,
    },
    # mapping symbol -> CMC id (chỉnh nếu cần)
    "cmc_symbol_ids": {
        "eth": 1027,
        "bnb": 1839,
        "xrp": 52,
    },
}


TELE_CONFIG = {
    "tele_bot_token": os.getenv("TELE_BOT_TOKEN"),
    "tele_chat_id": os.getenv("TELE_CHAT_ID"),
    "tele_message_parse": "HTML",
}
