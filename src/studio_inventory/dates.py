from __future__ import annotations

from datetime import datetime
import re

_MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

def _try_strptime(s: str, fmts: list[str]) -> datetime | None:
    for fmt in fmts:
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            pass
    return None

def normalize_datetime_iso(value: str | None) -> str | None:
    """Normalize many vendor date formats into an ISO-8601 string.

    Output:
      - If time is present: YYYY-MM-DDTHH:MM:SS
      - If only a date:    YYYY-MM-DD

    Notes:
      - Uses naive datetimes (no timezone). For this app we only need consistent sorting.
      - Returns None if parsing fails.
    """
    if not value:
        return None
    s = str(value).strip()
    if not s:
        return None

    # Already ISO-ish
    dt = _try_strptime(s, ["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M"])
    if dt:
        if re.search(r"\d{2}:\d{2}", s):
            return dt.strftime("%Y-%m-%dT%H:%M:%S")
        return dt.strftime("%Y-%m-%d")

    # Common US numeric formats
    dt = _try_strptime(s, ["%m/%d/%Y", "%m/%d/%y", "%m-%d-%Y", "%m-%d-%y"])
    if dt:
        return dt.strftime("%Y-%m-%d")

    # Month name formats with optional time:
    #   Aug 25, 2025
    #   Sep 3, 2025 6:12 PM
    m = re.match(r"^([A-Za-z]{3,9})\s+(\d{1,2}),\s*(\d{4})(?:\s+(\d{1,2}:\d{2})\s*([AaPp][Mm]))?$", s)
    if m:
        mon_s, day_s, year_s, time_s, ampm = m.groups()
        mon = _MONTHS.get(mon_s[:3].lower())
        if mon:
            day = int(day_s)
            year = int(year_s)
            if time_s and ampm:
                t = datetime.strptime(f"{time_s} {ampm.upper()}", "%I:%M %p")
                dt = datetime(year, mon, day, t.hour, t.minute, 0)
                return dt.strftime("%Y-%m-%dT%H:%M:%S")
            dt = datetime(year, mon, day)
            return dt.strftime("%Y-%m-%d")

    # DigiKey-ish: 20-SEP-2025
    m = re.match(r"^(\d{1,2})-([A-Za-z]{3})-(\d{4})$", s)
    if m:
        d, mon_s, y = m.groups()
        mon = _MONTHS.get(mon_s.lower())
        if mon:
            dt = datetime(int(y), mon, int(d))
            return dt.strftime("%Y-%m-%d")

    return None

def pretty_date(value: str | None) -> str:
    """Format ISO output from normalize_datetime_iso() into the UX-friendly date column.

    - YYYY-MM-DD           -> MM/DD/YYYY
    - YYYY-MM-DDTHH:MM:SS  -> MM/DD/YYYY\nH:MM AM
    Otherwise returns the original string (or empty).
    """
    if not value:
        return ""
    s = str(value).strip()
    if not s:
        return ""

    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", s)
    if m:
        y, mo, d = m.groups()
        return f"{mo}/{d}/{y}"

    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})$", s)
    if m:
        y, mo, d, hh, mm, ss = m.groups()
        dt = datetime(int(y), int(mo), int(d), int(hh), int(mm), int(ss))
        # %-I is mac/linux; if it fails somewhere, it will still show 01:23 etc (acceptable)
        return dt.strftime("%m/%d/%Y\n%-I:%M %p")

    return s
