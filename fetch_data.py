import os
import sys
import json
import math
import time
import random
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import requests
from bs4 import BeautifulSoup

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)
HISTORY_LOG_FILE = DATA_DIR / "history_log.json"
OPENING_TIMES_CACHE_FILE = DATA_DIR / "opening_times_cache.json"

API_KEY = os.getenv("TANKERKOENIG_API_KEY", "")
LAT = float(os.getenv("LOCATION_LAT", "50.5558"))
LNG = float(os.getenv("LOCATION_LNG", "8.5044"))
LOCATION_NAME = os.getenv("LOCATION_NAME", "Wetzlar")
RADIUS_KM = float(os.getenv("SEARCH_RADIUS_KM", "16"))

LIST_URL = "https://creativecommons.tankerkoenig.de/json/list.php"
DETAIL_URL = "https://creativecommons.tankerkoenig.de/json/detail.php"

ROTH_HVO_SOURCE = {
    "name_match": "Roth- Energie",
    "place_match": "Wetzlar",
    "station_page": "https://www.roth-energie.de/standorte-dev/ts_wetzlar_dillfeld_19",
    "price_page": "https://www.clever-tanken.de/spritpreise/hvodiesel-preise/wetzlar",
    "fuel_label": "HVO 100 Diesel",
    "price_source": "Clever Tanken / öffentliche HVO-Seite",
}

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("tankstellen-monitor")

MAX_RETRIES = int(os.getenv("TK_MAX_RETRIES", "3"))
BACKOFF_BASE_SECONDS = float(os.getenv("TK_BACKOFF_BASE_SECONDS", "2.0"))
BACKOFF_JITTER_SECONDS = float(os.getenv("TK_BACKOFF_JITTER_SECONDS", "0.4"))
TRANSIENT_HTTP_STATUSES = {429, 500, 502, 503, 504}
LIST_TIMEOUT_SECONDS = float(os.getenv("TK_LIST_TIMEOUT_SECONDS", "30"))
DETAIL_TIMEOUT_SECONDS = float(os.getenv("TK_DETAIL_TIMEOUT_SECONDS", "15"))
DETAIL_SLEEP_SECONDS = float(os.getenv("TK_DETAIL_SLEEP_SECONDS", "0.25"))
DETAIL_ENABLED = os.getenv("TK_DETAIL_ENABLED", "1").lower() not in ("0", "false", "no")
DETAIL_MAX_STATIONS = int(os.getenv("TK_DETAIL_MAX_STATIONS", "25"))
WRITE_PLACEHOLDERS_ON_ERROR = os.getenv("TK_WRITE_PLACEHOLDERS_ON_ERROR", "1").lower() not in ("0", "false", "no")
FAIL_FAST = os.getenv("FAIL_FAST", "0").lower() in ("1", "true", "yes")
USER_AGENT = os.getenv("HTTP_USER_AGENT", "tankstellen-monitor-leun/1.0 (GitHub Actions)")

SESSION = requests.Session()

def save_json(name: str, data: Any) -> None:
    path = DATA_DIR / name
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)

def load_json(path, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def haversine(lat1, lon1, lat2, lon2):
    r = 6371.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.atan2(math.sqrt(a), math.sqrt(1 - a))

class TankerkoenigTransientError(RuntimeError):
    pass

def request_json_with_retry(url: str, params: Dict[str, Any], timeout: float) -> Dict[str, Any]:
    last_err: Optional[BaseException] = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = SESSION.get(url, params=params, timeout=timeout, headers={"User-Agent": USER_AGENT})
            if r.status_code in TRANSIENT_HTTP_STATUSES:
                raise TankerkoenigTransientError(f"Transient HTTP {r.status_code}")
            r.raise_for_status()
            return r.json()
        except (requests.Timeout, requests.ConnectionError, TankerkoenigTransientError, ValueError) as e:
            last_err = e
            if attempt < MAX_RETRIES:
                sleep_s = BACKOFF_BASE_SECONDS * (2 ** (attempt - 1)) + random.random() * BACKOFF_JITTER_SECONDS
                log.warning("Request failed (attempt %s/%s) %s: %s; retry in %.1fs", attempt, MAX_RETRIES, url, e, sleep_s)
                time.sleep(sleep_s)
                continue
            break
    raise RuntimeError(f"Request failed after {MAX_RETRIES} attempts: {last_err}")

def tankerkoenig_list():
    if not API_KEY:
        raise RuntimeError("Missing TANKERKOENIG_API_KEY (Secret nicht gesetzt?)")
    params = {
        "lat": LAT,
        "lng": LNG,
        "rad": RADIUS_KM,
        "sort": "dist",
        "type": "all",
        "apikey": API_KEY,
    }
    data = request_json_with_retry(LIST_URL, params=params, timeout=LIST_TIMEOUT_SECONDS)
    if not data.get("ok"):
        msg = data.get("message") or str(data)
        raise RuntimeError(f"Tankerkönig list.php ok:false: {msg}")
    return data.get("stations", [])

def load_opening_times_cache():
    cache = load_json(OPENING_TIMES_CACHE_FILE, {})
    fetched = cache.get("_fetched_date")
    today = datetime.now().date().isoformat()
    if fetched == today:
        return cache
    return {}

def save_opening_times_cache(cache):
    cache["_fetched_date"] = datetime.now().date().isoformat()
    save_json("opening_times_cache.json", cache)

def tankerkoenig_detail(station_id, cache):
    if not DETAIL_ENABLED:
        return {}
    if station_id in cache:
        return cache[station_id]
    try:
        time.sleep(DETAIL_SLEEP_SECONDS)
        params = {"id": station_id, "apikey": API_KEY}
        r = SESSION.get(DETAIL_URL, params=params, timeout=DETAIL_TIMEOUT_SECONDS, headers={"User-Agent": USER_AGENT})
        if r.status_code in TRANSIENT_HTTP_STATUSES:
            log.info("detail.php transient HTTP %s for %s -> skip", r.status_code, station_id)
            return {}
        r.raise_for_status()
        data = r.json()
        if not data.get("ok"):
            return {}
        detail = data.get("station", {}) or {}
        cache[station_id] = detail
        return detail
    except Exception as e:
        log.info("Detail API error for %s: %s (skip)", station_id, e)
        return {}

def weekday_num(dt):
    return dt.weekday()

def parse_hm(value):
    try:
        hh, mm, *_ = str(value).split(":")
        return int(hh), int(mm)
    except Exception:
        return None

def minutes_of(value):
    hm = parse_hm(value)
    if not hm:
        return None
    return hm[0] * 60 + hm[1]

def format_hm(value):
    hm = parse_hm(value or "")
    if not hm:
        return None
    return f"{hm[0]:02d}:{hm[1]:02d} Uhr"

def extract_day_numbers(entry):
    days = entry.get("days")
    if isinstance(days, list) and days:
        out = []
        for d in days:
            if isinstance(d, int):
                out.append(d)
            elif isinstance(d, str) and d.isdigit():
                out.append(int(d))
        if out:
            return out
    text = (entry.get("text") or "").lower()
    mapping = {
        0: ["mo", "montag"], 1: ["di", "dienstag"], 2: ["mi", "mittwoch"],
        3: ["do", "donnerstag"], 4: ["fr", "freitag"], 5: ["sa", "samstag"], 6: ["so", "sonntag"]
    }
    if "mo-fr" in text or "montag-freitag" in text:
        return [0, 1, 2, 3, 4]
    if "sa-so" in text or "samstag-sonntag" in text or "wochenende" in text:
        return [5, 6]
    found = [k for k, vals in mapping.items() if any(v in text for v in vals)]
    return found

def todays_intervals(detail, now_local):
    result = []
    for entry in (detail.get("openingTimes") or []):
        if weekday_num(now_local) not in extract_day_numbers(entry):
            continue
        start = entry.get("start") or entry.get("opens") or entry.get("from")
        end = entry.get("end") or entry.get("closes") or entry.get("to")
        sm = minutes_of(start)
        em = minutes_of(end)
        if sm is None or em is None:
            continue
        result.append({"start": start, "end": end, "start_m": sm, "end_m": em})
    result.sort(key=lambda x: x["start_m"])
    return result

def compute_open_label(detail, now_local):
    if not detail:
        return None
    if detail.get("wholeDay") is True:
        return "Heute 24 Stunden geöffnet"
    now_m = now_local.hour * 60 + now_local.minute
    intervals = todays_intervals(detail, now_local)
    is_open = detail.get("isOpen")
    if is_open is True:
        current = None
        for iv in intervals:
            if iv["start_m"] <= now_m <= iv["end_m"]:
                current = iv
                break
        if current and current.get("end"):
            end_text = format_hm(current["end"])
            if end_text:
                return f"Geöffnet bis {end_text}"
        if intervals:
            end_text = format_hm(intervals[-1]["end"])
            if end_text:
                return f"Geöffnet bis {end_text}"
        return "Aktuell geöffnet"
    for iv in intervals:
        if now_m < iv["start_m"]:
            start_text = format_hm(iv["start"])
            if start_text:
                return f"Öffnet heute um {start_text}"
    return "Aktuell geschlossen"

def fetch_clever_tanken_hvo_price():
    try:
        r = SESSION.get(ROTH_HVO_SOURCE["price_page"], timeout=30, headers={"User-Agent": USER_AGENT})
        r.raise_for_status()
        text = r.text
        if "Roth- Energie, Dillfeld 19, 35576 Wetzlar" not in text:
            return None
        soup = BeautifulSoup(text, "html.parser")
        body_text = soup.get_text(" ", strip=True)
        import re
        m = re.search(r"Roth- Energie, Dillfeld 19, 35576 Wetzlar.*?(\d+[\.,]\d{3})", body_text)
        if m:
            return float(m.group(1).replace(",", "."))
        return None
    except Exception:
        return None

def enrich_hvo(stations):
    hvo_price = fetch_clever_tanken_hvo_price()
    notes = []
    for st in stations:
        st["hvo_available"] = False
        st["hvo_source"] = None
        st["hvo_note"] = None
        if ROTH_HVO_SOURCE["name_match"].lower() in (st.get("brand") or "").lower() or ROTH_HVO_SOURCE["name_match"].lower() in (st.get("name") or "").lower():
            if ROTH_HVO_SOURCE["place_match"].lower() in (st.get("place") or "").lower():
                st["hvo_available"] = True
                st["hvo_product_name"] = ROTH_HVO_SOURCE["fuel_label"]
                st["hvo_source"] = ROTH_HVO_SOURCE["price_source"]
                if hvo_price is not None:
                    st["hvo"] = hvo_price
                    st["hvo_note"] = "HVO-Sonderquelle aktiv"
                    notes.append({"station": st.get("name"), "status": "price_found", "price": hvo_price})
                else:
                    st["hvo"] = None
                    st["hvo_note"] = "HVO laut Betreiber verfügbar, Preis extern derzeit nicht sicher extrahierbar"
                    notes.append({"station": st.get("name"), "status": "available_without_price"})
    return notes

def build_current_data():
    stations = tankerkoenig_list()
    current = []
    now_dt = datetime.now(timezone.utc).astimezone()
    now = now_dt.isoformat()
    opening_cache = load_opening_times_cache()

    for i, s in enumerate(stations):
        do_detail = bool(s.get("id")) and (i < DETAIL_MAX_STATIONS)
        detail = tankerkoenig_detail(s.get("id"), opening_cache) if do_detail else {}
        current.append({
            "id": s.get("id"),
            "name": s.get("name"),
            "brand": s.get("brand"),
            "street": s.get("street"),
            "houseNumber": s.get("houseNumber"),
            "postCode": s.get("postCode"),
            "place": s.get("place"),
            "lat": s.get("lat"),
            "lng": s.get("lng"),
            "distance_km": round(float(s.get("dist") or haversine(LAT, LNG, s.get("lat"), s.get("lng"))), 2),
            "is_open": s.get("isOpen"),
            "opening_times": detail.get("openingTimes") or [],
            "opening_overrides": detail.get("overrides") or [],
            "whole_day": detail.get("wholeDay"),
            "open_label": compute_open_label(detail, now_dt),
            "diesel": s.get("diesel"),
            "e5": s.get("e5"),
            "e10": s.get("e10"),
            "hvo": None,
            "last_update": now,
        })

    save_opening_times_cache(opening_cache)
    notes = enrich_hvo(current)
    current.sort(key=lambda x: (x.get("distance_km") is None, x.get("distance_km", 9999)))
    return current, notes

def average_for(stations, fuel):
    vals = [s.get(fuel) for s in stations if isinstance(s.get(fuel), (int, float))]
    return round(sum(vals) / len(vals), 3) if vals else None

def update_history_log(stations, averages):
    log_items = load_json(HISTORY_LOG_FILE, [])
    now = datetime.now().isoformat(timespec="seconds")
    new_entry = {
        "timestamp": now,
        "averages": averages,
        "stations": [
            {
                "id": s.get("id"),
                "name": s.get("name"),
                "brand": s.get("brand"),
                "diesel": s.get("diesel"),
                "e5": s.get("e5"),
                "e10": s.get("e10"),
                "hvo": s.get("hvo"),
                "is_open": s.get("is_open"),
            }
            for s in stations
        ]
    }
    if not log_items or log_items[-1].get("timestamp") != now:
        log_items.append(new_entry)
    cutoff = datetime.now() - timedelta(days=60)
    trimmed = []
    for item in log_items:
        try:
            ts = datetime.fromisoformat(item.get("timestamp"))
            if ts >= cutoff:
                trimmed.append(item)
        except Exception:
            continue
    save_json("history_log.json", trimmed)
    return trimmed

def build_history_from_log(history_log, fuel, days):
    cutoff = datetime.now() - timedelta(days=days)
    grouped = {}
    for item in history_log:
        try:
            ts = datetime.fromisoformat(item.get("timestamp"))
        except Exception:
            continue
        if ts < cutoff:
            continue
        day = ts.date().isoformat()
        day_bucket = grouped.setdefault(day, {"prices": [], "snapshot_avgs": []})
        stations = item.get("stations") or []
        open_prices = []
        for st in stations:
            if st.get("is_open") is not True:
                continue
            price = st.get(fuel)
            if isinstance(price, (int, float)):
                open_prices.append(float(price))
                day_bucket["prices"].append(float(price))
        if open_prices:
            day_bucket["snapshot_avgs"].append(sum(open_prices) / len(open_prices))
    rows = []
    for day in sorted(grouped.keys()):
        vals = grouped[day]["prices"]
        snap = grouped[day]["snapshot_avgs"]
        if not vals:
            continue
        rows.append({
            "day": day,
            "avg_price": round(sum(vals) / len(vals), 3),
            "min_price": round(min(snap), 3) if snap else round(min(vals), 3),
            "max_price": round(max(snap), 3) if snap else round(max(vals), 3),
            "readings": len(vals),
            "snapshots": len(snap),
        })
    return rows

def build_best_times(history_log, days=30):
    cutoff = datetime.now() - timedelta(days=days)
    fuels = ["diesel", "e5", "e10", "hvo"]
    buckets = {fuel: {} for fuel in fuels}
    for item in history_log:
        try:
            ts = datetime.fromisoformat(item.get("timestamp"))
        except Exception:
            continue
        if ts < cutoff:
            continue
        slot = f"{ts.hour:02d}:{(ts.minute // 30) * 30:02d}"
        stations = item.get("stations") or []
        for fuel in fuels:
            vals = []
            for st in stations:
                if st.get("is_open") is not True:
                    continue
                price = st.get(fuel)
                if isinstance(price, (int, float)):
                    vals.append(float(price))
            if not vals:
                continue
            slot_avg = sum(vals) / len(vals)
            buckets[fuel].setdefault(slot, []).append(slot_avg)
    result = {}
    for fuel, slots in buckets.items():
        stats = []
        for slot, vals in sorted(slots.items()):
            stats.append({
                "slot": slot,
                "avg_price": round(sum(vals) / len(vals), 3),
                "samples": len(vals),
            })
        best = min(stats, key=lambda x: x["avg_price"]) if stats else None
        result[fuel] = {
            "window_days": days,
            "best_slot": best["slot"] if best else None,
            "best_price": best["avg_price"] if best else None,
            "samples": best["samples"] if best else 0,
            "enough_data": len(stats) >= 4 and sum(s["samples"] for s in stats) >= 8,
            "slots": stats,
        }
    return result

def build_changes(stations, hours):
    history_log = load_json(HISTORY_LOG_FILE, [])
    cutoff = datetime.now() - timedelta(hours=hours)
    rows = []
    for s in stations:
        sid = s.get("id")
        historic_entries = []
        for entry in history_log:
            try:
                ts = datetime.fromisoformat(entry["timestamp"])
            except Exception:
                continue
            if ts <= cutoff:
                for st in entry.get("stations", []):
                    if st.get("id") == sid:
                        historic_entries.append((ts, st))
        if historic_entries:
            historic_entries.sort(key=lambda x: x[0], reverse=True)
            _, hist = historic_entries[0]
        else:
            hist = None

        def calc_change(fuel):
            cur = s.get(fuel)
            if cur is None or hist is None:
                return None
            old_val = hist.get(fuel)
            if old_val is None:
                return None
            diff = round(cur - old_val, 3)
            return diff if diff != 0 else 0.0

        change = {
            "diesel": calc_change("diesel"),
            "e5": calc_change("e5"),
            "e10": calc_change("e10"),
            "hvo": calc_change("hvo"),
        }
        if any(v is not None for v in change.values()):
            rows.append({
                "station_id": sid,
                "station_name": s.get("name"),
                "brand": s.get("brand"),
                "distance_km": s.get("distance_km"),
                "current": {
                    "diesel": s.get("diesel"),
                    "e5": s.get("e5"),
                    "e10": s.get("e10"),
                    "hvo": s.get("hvo"),
                    "timestamp": s.get("last_update"),
                },
                "change": change,
            })
    rows.sort(key=lambda r: -max((abs(v) for v in r["change"].values() if v is not None), default=0))
    return rows

def ensure_minimal_files_exist():
    defaults = {
        "stations_prices.json": [],
        "averages.json": {},
        "best_times_30d.json": {},
        "changes_24h.json": [],
        "changes_48h.json": [],
        "history_log.json": [],
        "opening_times_cache.json": {},
    }
    for name, default in defaults.items():
        p = DATA_DIR / name
        if not p.exists():
            save_json(name, default)
    for fuel in ["diesel", "e5", "e10", "hvo"]:
        for days in [7, 14, 30, 60]:
            name = f"history_{fuel}_{days}.json"
            p = DATA_DIR / name
            if not p.exists():
                save_json(name, [])

def main():
    stations, hvo_notes = build_current_data()
    averages = {
        "diesel": average_for(stations, "diesel"),
        "e5": average_for(stations, "e5"),
        "e10": average_for(stations, "e10"),
        "hvo": average_for(stations, "hvo"),
    }
    history_log = update_history_log(stations, averages)
    status = {
        "ok": True,
        "location": LOCATION_NAME,
        "radius_km": RADIUS_KM,
        "last_fetch": datetime.now().isoformat(timespec="seconds"),
        "station_count": len(stations),
        "history_points": len(history_log),
        "hvo_strategy": "Tankerkönig für Standardkraftstoffe, Sonderquelle für bekannte HVO-Stationen",
        "hvo_notes": hvo_notes,
    }
    save_json("stations_prices.json", stations)
    save_json("averages.json", averages)
    save_json("changes_24h.json", build_changes(stations, 24))
    save_json("changes_48h.json", build_changes(stations, 48))
    save_json("status.json", status)
    save_json("history_raw.json", stations)
    for fuel in ["diesel", "e5", "e10", "hvo"]:
        for days in [7, 14, 30, 60]:
            save_json(f"history_{fuel}_{days}.json", build_history_from_log(history_log, fuel, days))
    save_json("best_times_30d.json", build_best_times(history_log, 30))

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.exception("FATAL ERROR: %s", e)
        try:
            prev_status = load_json(DATA_DIR / "status.json", {})
            last_success = prev_status.get("last_fetch") or prev_status.get("last_success")
        except Exception:
            last_success = None
        try:
            DATA_DIR.mkdir(exist_ok=True)
            err_status = {
                "ok": False,
                "location": LOCATION_NAME,
                "radius_km": RADIUS_KM,
                "error": str(e),
                "last_attempt": datetime.now().isoformat(timespec="seconds"),
                "last_success": last_success,
            }
            save_json("status.json", err_status)
            if WRITE_PLACEHOLDERS_ON_ERROR:
                ensure_minimal_files_exist()
        except Exception as inner:
            log.error("Could not write error status/placeholder files: %s", inner)
        if FAIL_FAST:
            raise
        sys.exit(0)
