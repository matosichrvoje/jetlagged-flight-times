#!/usr/bin/env python3
"""Nightly: turn one day of adsb.lol traces into "usual times" per flight number.

Reads a readsb globe_history tar (one gzip JSON per aircraft per UTC day) from
stdin, finds every take-off and landing near an airport, and folds them into
a rolling state of recent observations per callsign. Then writes one small
JSON file per airline (ICAO prefix) with each route's typical local departure
time and duration.

    curl -sL <...tar.aa> <...tar.ab> | python3 build.py --date 2026-11-21 --site site

Data: adsb.lol, ODbL 1.0. Output is a derived database under the same licence.
"""
import argparse, gzip, io, json, math, os, statistics, sys, tarfile
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

HERE = os.path.dirname(os.path.abspath(__file__))
KEEP_DAYS = 35          # rolling window of observations
MAX_OBS = 24            # per callsign+route
AIRPORT_KM = 12         # how close the first/last point must be to an airport
LOW_FT = 4000           # first/last point must be below this (or on the ground)
NEW_LEG = 2             # readsb trace flag: this point starts a new leg
GAP_S = 4 * 3600        # a silence this long also splits legs
TAXI_OUT = 15 * 60      # wheels-up → typical gate departure on the ticket
TAXI_IN = 8 * 60        # touchdown → typical gate arrival

# ---------- airports ----------

AIRPORTS = json.load(open(os.path.join(HERE, "airports.json")))  # [icao, lat, lon, tz]
GRID = defaultdict(list)
for a in AIRPORTS:
    GRID[(math.floor(a[1]), math.floor(a[2]))].append(a)


def km(lat1, lon1, lat2, lon2):
    p = math.pi / 180
    a = (math.sin((lat2 - lat1) * p / 2) ** 2
         + math.cos(lat1 * p) * math.cos(lat2 * p) * math.sin((lon2 - lon1) * p / 2) ** 2)
    return 12742 * math.asin(math.sqrt(a))


def nearest(lat, lon):
    best, bd = None, AIRPORT_KM
    for dy in (-1, 0, 1):
        for dx in (-1, 0, 1):
            for a in GRID.get((math.floor(lat) + dy, math.floor(lon) + dx), ()):
                d = km(lat, lon, a[1], a[2])
                if d < bd:
                    best, bd = a, d
    return best


def low(p):
    alt = p[3]
    return alt == "ground" or (isinstance(alt, (int, float)) and alt < LOW_FT)

# ---------- one aircraft ----------

def legs(doc):
    """Yield (callsign, start_point, end_point, base_ts) for each leg in a trace."""
    base = doc["timestamp"]
    trace = doc.get("trace") or []
    cur, prev_t, calls = [], None, defaultdict(int)

    def flush():
        if len(cur) >= 2 and calls:
            cs = max(calls, key=calls.get)
            yield cs, cur[0], cur[-1]

    for p in trace:
        t = p[0]
        if cur and ((p[6] or 0) & NEW_LEG or (prev_t is not None and t - prev_t > GAP_S)):
            yield from flush()
            cur, calls = [], defaultdict(int)
        cur.append(p)
        det = p[8]
        if det and det.get("flight"):
            cs = det["flight"].strip()
            if cs:
                calls[cs] += 1
        prev_t = t
    yield from flush()


def scan(raw):
    """Parse one trace file → (finished flights, open take-offs, open landings)."""
    try:
        doc = json.loads(gzip.decompress(raw))
    except Exception:
        try:
            doc = json.loads(raw)
        except Exception:
            return [], [], []
    hexid, base = doc.get("icao"), doc.get("timestamp", 0)
    done, opened, landed = [], [], []
    for cs, a, b in legs(doc):
        if not (cs[:3].isalpha() and any(c.isdigit() for c in cs)):
            continue  # airline-style callsigns only: ABC123
        t0, t1 = base + a[0], base + b[0]
        o = nearest(a[1], a[2]) if low(a) else None
        d = nearest(b[1], b[2]) if low(b) else None
        if o and d and o[0] != d[0] and t1 - t0 > 20 * 60:
            done.append((cs, o[0], d[0], t0, t1))
        elif o and not d and base + 86400 - t1 < 30 * 60:
            opened.append((hexid, cs, o[0], t0))          # still flying at midnight
        elif d and not o and t0 - base < 30 * 60:
            landed.append((hexid, cs, d[0], t1))          # was flying at midnight
    return done, opened, landed


def members(stream):
    with tarfile.open(fileobj=stream, mode="r|") as tar:
        for m in tar:
            if m.isfile() and "/traces/" in "/" + m.name and "trace_full_" in m.name:
                f = tar.extractfile(m)
                if f:
                    yield f.read()

# ---------- state + output ----------

def load(path, default):
    try:
        with open(path) as f:
            return json.load(f)
    except FileNotFoundError:
        return default


def local_minutes(ts, tz):
    t = datetime.fromtimestamp(ts, timezone.utc).astimezone(ZoneInfo(tz))
    return t.hour * 60 + t.minute, t.isoweekday()


def circular_median(mins):
    """Median of minutes-of-day that may wrap past midnight."""
    ref = mins[0]
    shifted = [((m - ref + 720) % 1440) - 720 for m in mins]
    return int(round(ref + statistics.median(shifted))) % 1440


def hhmm(m):
    return f"{m // 60:02d}:{m % 60:02d}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="UTC day of the archive, YYYY-MM-DD")
    ap.add_argument("--site", required=True, help="output dir (also holds state/)")
    ap.add_argument("--workers", type=int, default=os.cpu_count() or 2)
    args = ap.parse_args()

    tz_of = {a[0]: a[3] for a in AIRPORTS}
    state_dir = os.path.join(args.site, "state")
    os.makedirs(state_dir, exist_ok=True)
    obs = load(os.path.join(state_dir, "obs.json"), {})          # "CS|O|D" -> [[dep, arr], ...]
    pending = load(os.path.join(state_dir, "pending.json"), [])  # take-offs open at last midnight

    flights, opened, landed, files = [], [], [], 0
    with ProcessPoolExecutor(args.workers) as pool:
        for done, o, l in pool.map(scan, members(sys.stdin.buffer), chunksize=64):
            flights += done; opened += o; landed += l
            files += 1
    print(f"{args.date}: {files} aircraft, {len(flights)} flights, "
          f"{len(opened)} open at midnight, {len(landed)} landed after midnight", file=sys.stderr)

    # Join yesterday's open take-offs with today's after-midnight landings.
    by_hex = {(p[0], p[1]): p for p in pending}
    for hexid, cs, dest, t1 in landed:
        p = by_hex.get((hexid, cs))
        if p and p[2] != dest:
            flights.append((cs, p[2], dest, p[3], t1))

    for cs, o, d, t0, t1 in flights:
        obs.setdefault(f"{cs}|{o}|{d}", []).append([int(t0 - TAXI_OUT), int(t1 + TAXI_IN)])

    cutoff = datetime.fromisoformat(args.date).replace(tzinfo=timezone.utc).timestamp() - KEEP_DAYS * 86400
    for k in list(obs):
        kept = sorted({tuple(x) for x in obs[k] if x[0] >= cutoff})[-MAX_OBS:]
        if kept:
            obs[k] = [list(x) for x in kept]
        else:
            del obs[k]

    # Publish: v1/<AIRLINE>.json → {callsign: [route, ...]} most-flown first.
    airlines = defaultdict(lambda: defaultdict(list))
    for k, xs in obs.items():
        cs, o, d = k.split("|")
        if o not in tz_of or d not in tz_of:
            continue
        deps = [local_minutes(x[0], tz_of[o]) for x in xs]
        durs = [round((x[1] - x[0]) / 60) for x in xs]
        dep = circular_median([m for m, _ in deps])
        dur = int(statistics.median(durs))
        arr = local_minutes(xs[-1][0] + dur * 60, tz_of[d])[0]
        # Adjust arrival to the typical departure, not the last one.
        last_dep_local = local_minutes(xs[-1][0], tz_of[o])[0]
        arr = (arr + (dep - last_dep_local)) % 1440
        airlines[cs[:3]][cs].append({
            "o": o, "d": d,
            "dep": hhmm(dep), "arr": hhmm(arr), "min": dur,
            "n": len(xs),
            "days": sorted({wd for _, wd in deps}),
            "last": datetime.fromtimestamp(xs[-1][0], timezone.utc).strftime("%Y-%m-%d"),
        })

    out_dir = os.path.join(args.site, "v1")
    os.makedirs(out_dir, exist_ok=True)
    for f in os.listdir(out_dir):
        os.remove(os.path.join(out_dir, f))
    for airline, calls in airlines.items():
        for routes in calls.values():
            routes.sort(key=lambda r: -r["n"])
        with open(os.path.join(out_dir, f"{airline}.json"), "w") as f:
            json.dump(calls, f, separators=(",", ":"), sort_keys=True)

    with open(os.path.join(state_dir, "obs.json"), "w") as f:
        json.dump(obs, f, separators=(",", ":"))
    with open(os.path.join(state_dir, "pending.json"), "w") as f:
        json.dump(opened, f, separators=(",", ":"))
    with open(os.path.join(args.site, "meta.json"), "w") as f:
        json.dump({"updated": args.date, "airlines": len(airlines), "routes": len(obs),
                   "source": "adsb.lol (ODbL 1.0)"}, f)
    print(f"published {len(airlines)} airlines, {len(obs)} callsign routes", file=sys.stderr)


if __name__ == "__main__":
    main()
