import json
import math
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
import requests

API_KEY = os.getenv("TANKERKOENIG_API_KEY", "")
LAT = float(os.getenv("LOCATION_LAT", "50.5547"))
LNG = float(os.getenv("LOCATION_LNG", "8.3890"))
LOCATION_NAME = os.getenv("LOCATION_NAME", "Justengarten 4A, 35638 Leun")
RADIUS_KM = float(os.getenv("SEARCH_RADIUS_KM", "16"))
DATA_DIR = Path("data")
BASE_URL = "https://creativecommons.tankerkoenig.de/json/list.php"
NOW = datetime.now(timezone.utc).astimezone()

DATA_DIR.mkdir(parents=True, exist_ok=True)

def distance_km(lat1, lon1, lat2, lon2):
    r = 6371.0
    p1 = math.radians(lat1); p2 = math.radians(lat2)
    dp = math.radians(lat2-lat1); dl = math.radians(lon2-lon1)
    a = math.sin(dp/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2
    return r * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

def load_json(name, default):
    p = DATA_DIR / name
    if p.exists():
        return json.loads(p.read_text(encoding="utf-8"))
    return default

def save_json(name, payload):
    (DATA_DIR / name).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

def fetch_stations():
    r = requests.get(BASE_URL, timeout=20, params={
        "lat": LAT,
        "lng": LNG,
        "rad": RADIUS_KM,
        "sort": "dist",
        "type": "all",
        "apikey": API_KEY,
    })
    r.raise_for_status()
    data = r.json()
    if not data.get("ok"):
        raise RuntimeError(f"Tankerkönig API Fehler: {data}")
    out = []
    for s in data.get("stations", []):
        dist = s.get("dist")
        if dist is None and s.get("lat") is not None and s.get("lng") is not None:
            dist = distance_km(LAT, LNG, s["lat"], s["lng"])
        if dist is None or dist > RADIUS_KM:
            continue
        out.append({
            "id": s.get("id"),
            "name": s.get("name"),
            "brand": s.get("brand"),
            "street": s.get("street"),
            "place": s.get("place"),
            "lat": s.get("lat"),
            "lng": s.get("lng"),
            "distance_km": round(dist, 2),
            "is_open": bool(s.get("isOpen")),
            "timestamp": NOW.isoformat(timespec="seconds"),
            "diesel": s.get("diesel"),
            "e5": s.get("e5"),
            "e10": s.get("e10"),
            "hvo": s.get("hvo"),
        })
    out.sort(key=lambda x: x["distance_km"])
    return out

def append_history(stations):
    history = load_json("history_raw.json", {"snapshots": []})
    history["snapshots"].append({"timestamp": NOW.isoformat(timespec="seconds"), "stations": stations})
    cutoff = NOW - timedelta(days=60)
    trimmed = []
    for snap in history["snapshots"]:
        try:
            ts = datetime.fromisoformat(snap["timestamp"])
            if ts >= cutoff:
                trimmed.append(snap)
        except Exception:
            pass
    history["snapshots"] = trimmed
    save_json("history_raw.json", history)
    return history

def build_averages(stations):
    result = {}
    for fuel in ["diesel", "e5", "e10", "hvo"]:
        vals = [s[fuel] for s in stations if s.get(fuel) is not None]
        result[fuel] = round(sum(vals)/len(vals), 3) if vals else None
    return {"ok": True, "averages": result}

def build_status(stations):
    return {
        "server": "Tankstellen-Monitor Leun",
        "location": LOCATION_NAME,
        "radius_km": RADIUS_KM,
        "schedule": "halbstündlich via GitHub Actions",
        "last_fetch": {"timestamp": NOW.isoformat(timespec="seconds"), "success": True},
        "count": len(stations),
    }

def aggregate_daily(history, fuel, days):
    cutoff = NOW - timedelta(days=days)
    by_day = {}
    for snap in history.get("snapshots", []):
        ts = datetime.fromisoformat(snap["timestamp"])
        if ts < cutoff:
            continue
        vals = [s.get(fuel) for s in snap.get("stations", []) if s.get(fuel) is not None]
        if not vals:
            continue
        day = ts.date().isoformat()
        by_day.setdefault(day, []).extend(vals)
    data = []
    for day in sorted(by_day):
        vals = by_day[day]
        data.append({
            "day": day,
            "avg_price": round(sum(vals)/len(vals), 3),
            "min_price": round(min(vals), 3),
            "max_price": round(max(vals), 3),
            "readings": len(vals),
        })
    return {"ok": True, "fuel": fuel, "days": days, "data": data}

def build_changes(history, hours):
    cutoff = NOW - timedelta(hours=hours)
    first = None
    last = None
    for snap in history.get("snapshots", []):
        ts = datetime.fromisoformat(snap["timestamp"])
        if ts >= cutoff and first is None:
            first = snap
        last = snap
    if not first or not last:
        return {"ok": True, "hours": hours, "changes": []}
    first_map = {s["id"]: s for s in first.get("stations", [])}
    last_map = {s["id"]: s for s in last.get("stations", [])}
    changes = []
    for sid, now_s in last_map.items():
        old_s = first_map.get(sid)
        if not old_s:
            continue
        change = {}
        for fuel in ["diesel", "e5", "e10", "hvo"]:
            a = old_s.get(fuel); b = now_s.get(fuel)
            change[fuel] = round(b-a, 4) if a is not None and b is not None else None
        changes.append({
            "station_id": sid,
            "station_name": now_s.get("name"),
            "brand": now_s.get("brand"),
            "distance_km": now_s.get("distance_km"),
            "current": {k: now_s.get(k) for k in ["diesel", "e5", "e10", "hvo", "timestamp"]},
            "change": change,
        })
    changes.sort(key=lambda x: x["distance_km"])
    return {"ok": True, "hours": hours, "changes": changes}

def main():
    if not API_KEY:
        raise RuntimeError("TANKERKOENIG_API_KEY fehlt")
    stations = fetch_stations()
    save_json("stations_prices.json", {"ok": True, "count": len(stations), "stations": stations})
    save_json("averages.json", build_averages(stations))
    save_json("status.json", build_status(stations))
    history = append_history(stations)
    for fuel in ["diesel", "e5", "e10", "hvo"]:
        for days in [7, 14, 30, 60]:
            save_json(f"history_{fuel}_{days}.json", aggregate_daily(history, fuel, days))
    save_json("changes_24h.json", build_changes(history, 24))
    save_json("changes_48h.json", build_changes(history, 48))
    print(f"Gespeichert: {len(stations)} Stationen")

if __name__ == "__main__":
    main()
