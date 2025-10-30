from datetime import datetime
from typing import Optional


class ConvertDatetime:
    """Lớp tiện ích chuyển đổi datetime từ các định dạng ISO sang 'YYYY-MM-DD HH:MM:SS'.

    Sử dụng: ConvertDatetime().iso_to_sql_datetime("2017-12-31T23:59:59.999Z")
    """

    def iso_to_sql_datetime(self, iso_str: Optional[str]) -> Optional[str]:
        """Chuyển các chuỗi ISO dạng '2017-12-31T23:59:59.999Z' hoặc '2017-12-31T23:59:59Z'
        về định dạng 'YYYY-MM-DD HH:MM:SS'. Trả về None nếu input là None.
        Nếu không parse được, trả về chuỗi gốc.
        """
        if iso_str is None:
            return None

        s = str(iso_str).strip()
        # loại bỏ Z cuối chuỗi nếu có
        if s.endswith("Z"):
            s = s[:-1]

        formats = [
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
        ]

        for fmt in formats:
            try:
                dt = datetime.strptime(s, fmt)
                return dt.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                continue

        try:
            dt = datetime.fromisoformat(s)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return iso_str


__all__ = ["ConvertDatetime"]
