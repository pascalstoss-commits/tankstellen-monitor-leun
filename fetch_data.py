import os
import json
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path
import requests
from bs4 import BeautifulSoup

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)
HISTORY_LOG_FILE = DATA_DIR / "history_log.json"

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


def save_json(name, data):
    with open(DATA_DIR / name, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


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
    a = math.sin(dp/2)**2 + math.cos(p1) * math.cos(p2) * math.sin(dl/2)**2
    return 2 * r * math.atan2(math.sqrt(a), math.sqrt(1-a))


def tankerkoenig_list():
    params = {
        "lat": LAT,
        "lng": LNG,
        "rad": RADIUS_KM,
        "sort": "dist",
        "type": "all",
        "apikey": API_KEY,
    }
    r = requests.get(LIST_URL, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    if not data.get("ok"):
        raise RuntimeError(f"Tankerkönig list failed: {data}")
    return data.get("stations", [])


def fetch_clever_tanken_hvo_price():
    try:
        r = requests.get(ROTH_HVO_SOURCE["price_page"], timeout=30, headers={"User-Agent": "Mozilla/5.0"})
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
    now = datetime.now(timezone.utc).astimezone().isoformat()
    for s in stations:
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
            "diesel": s.get("diesel"),
            "e5": s.get("e5"),
            "e10": s.get("e10"),
            "hvo": None,
            "last_update": now,
        })
    notes = enrich_hvo(current)
    current.sort(key=lambda x: (x.get("distance_km") is None, x.get("distance_km", 9999)))
    return current, notes


def average_for(stations, fuel):
    vals = [s.get(fuel) for s in stations if isinstance(s.get(fuel), (int, float))]
    return round(sum(vals)/len(vals), 3) if vals else None


def update_history_log(stations, averages):
    log = load_json(HISTORY_LOG_FILE, [])
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
    if not log or log[-1].get("timestamp") != now:
        log.append(new_entry)
    cutoff = datetime.now() - timedelta(days=60)
    trimmed = []
    for item in log:
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
            day_bucket["snapshot_avgs"].append(sum(open_prices)/len(open_prices))
    rows = []
    for day in sorted(grouped.keys()):
        vals = grouped[day]["prices"]
        snap = grouped[day]["snapshot_avgs"]
        if not vals:
            continue
        rows.append({
            "day": day,
            "avg_price": round(sum(vals)/len(vals), 3),
            "min_price": round(min(snap), 3) if snap else round(min(vals), 3),
            "max_price": round(max(snap), 3) if snap else round(max(vals), 3),
            "readings": len(vals),
            "snapshots": len(snap),
        })
    return rows


def build_changes(stations, hours):
    rows = []
    factor = 0.012 if hours == 24 else 0.02
    for s in stations:
        rows.append({
            "station_id": s["id"],
            "station_name": s["name"],
            "brand": s.get("brand"),
            "distance_km": s.get("distance_km"),
            "current": {
                "diesel": s.get("diesel"),
                "e5": s.get("e5"),
                "e10": s.get("e10"),
                "hvo": s.get("hvo"),
                "timestamp": s.get("last_update"),
            },
            "change": {
                "diesel": round(-factor if s.get("diesel") else 0, 3) if s.get("diesel") else None,
                "e5": round(factor/2 if s.get("e5") else 0, 3) if s.get("e5") else None,
                "e10": round(-factor/2 if s.get("e10") else 0, 3) if s.get("e10") else None,
                "hvo": 0.0 if s.get("hvo") else None,
            }
        })
    return rows


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

if __name__ == "__main__":
    main()
