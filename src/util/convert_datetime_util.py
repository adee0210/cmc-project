from datetime import datetime, timedelta
from typing import Optional


class ConvertDatetime:
    """Lớp tiện ích chuyển đổi datetime từ các định dạng ISO sang 'YYYY-MM-DD HH:MM:SS'.

    Thời gian được làm tròn lên phút gần nhất (ví dụ: 08:29:59 → 08:30:00).
    Sử dụng: ConvertDatetime().iso_to_sql_datetime("2017-12-31T23:59:59.999Z")
    """

    def iso_to_sql_datetime(self, iso_str: Optional[str]) -> Optional[str]:
        """Chuyển các chuỗi ISO dạng '2017-12-31T23:59:59.999Z' hoặc '2017-12-31T23:59:59Z'
        về định dạng 'YYYY-MM-DD HH:MM:SS' và làm tròn lên phút gần nhất.
        Trả về None nếu input là None. Nếu không parse được, trả về chuỗi gốc.
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
                # Làm tròn lên phút gần nhất
                dt_rounded = self._round_to_nearest_minute(dt)
                return dt_rounded.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                continue

        try:
            dt = datetime.fromisoformat(s)
            # Làm tròn lên phút gần nhất
            dt_rounded = self._round_to_nearest_minute(dt)
            return dt_rounded.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return iso_str

    def _round_to_nearest_minute(self, dt: datetime) -> datetime:
        """Làm tròn datetime lên phút gần nhất.

        Ví dụ: 08:29:59 → 08:30:00, 08:29:01 → 08:30:00
        """
        # Nếu có giây hoặc microsecond, làm tròn lên phút tiếp theo
        if dt.second > 0 or dt.microsecond > 0:
            dt = dt.replace(second=0, microsecond=0)
            dt = dt + timedelta(minutes=1)
        return dt


__all__ = ["ConvertDatetime"]
