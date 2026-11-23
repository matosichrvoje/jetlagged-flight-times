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

CRUISE_FT = 10000       # must climb above this between two airports
MAX_FLIGHT_S = 20 * 3600


def high(p):
    alt = p[3]
    return isinstance(alt, (int, float)) and alt >= CRUISE_FT


def scan(raw):
    """Walk one aircraft's day: airport → climb → different airport is a flight.

    Silences mid-flight (oceans, poor coverage) don't split anything; only
    being low near an airport does. Returns (flights, open at midnight,
    landed after midnight).
    """
    try:
        doc = json.loads(gzip.decompress(raw))
    except Exception:
        try:
            doc = json.loads(raw)
        except Exception:
            return [], [], []
    hexid, base = doc.get("icao"), doc.get("timestamp", 0)
    trace = doc.get("trace") or []
    cache = {}

    def airport_at(p):
        key = (round(p[1], 2), round(p[2], 2))
        if key not in cache:
            a = nearest(p[1], p[2])
            cache[key] = a[0] if a else None
        return cache[key]

    done, opened, landed = [], [], []
    origin, dep_t = None, None       # last airport we sat at, and when we left it
    climbed = False                  # been at cruise since leaving origin
    calls = defaultdict(int)
    seen_low_yet = False             # first low point of the day not reached yet

    def callsign():
        return max(calls, key=calls.get) if calls else None

    for p in trace:
        t = base + p[0]
        det = p[8]
        if det and det.get("flight") and not low(p):
            cs = det["flight"].strip()    # airborne only: parked jets keep the last flight's number
            if cs:
                calls[cs] += 1
        if low(p):
            ap = airport_at(p)
            if ap is None:
                continue
            if not seen_low_yet and origin is None and climbed:
                # Day began in the air: landing of a flight from yesterday.
                cs = callsign()
                if cs:
                    landed.append((hexid, cs, ap, t))
            elif origin and climbed and ap != origin:
                cs = callsign()
                if cs and 20 * 60 < t - dep_t < MAX_FLIGHT_S:
                    done.append((cs, origin, ap, dep_t, t))
            seen_low_yet = True
            if ap != origin or climbed:
                calls = defaultdict(int)
            origin, dep_t, climbed = ap, t, False
        elif high(p):
            if origin is None and not seen_low_yet:
                climbed = True       # airborne since before midnight
            elif origin:
                climbed = True

    # Still airborne at the end of the day.
    if origin and climbed and trace and base + 86400 - (base + trace[-1][0]) < 3 * 3600:
        cs = callsign()
        if cs:
            opened.append((hexid, cs, origin, dep_t))

    keep = lambda cs: cs[:3].isalpha() and any(c.isdigit() for c in cs)
    return ([f for f in done if keep(f[0])],
            [o for o in opened if keep(o[1])],
            [l for l in landed if keep(l[1])])


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
