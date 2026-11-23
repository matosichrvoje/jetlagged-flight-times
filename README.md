# Flight times

Typical departure and arrival times per flight number, rebuilt every night
from open ADS-B tracking data. Used by the Jetlagged app to suggest times
after you type a flight number.

## Files

Published to GitHub Pages:

- `v1/<AIRLINE>.json`: one file per airline ICAO code (`UAE.json` for Emirates).
  Each entry maps a callsign to its routes, most flown first:

  ```json
  { "UAE255": [ { "o": "LEBL", "d": "MMMX", "dep": "10:10", "arr": "16:00",
                  "min": 830, "n": 21, "days": [1,2,3,4,5,6,7], "last": "2026-11-21" } ] }
  ```

  `dep` and `arr` are local times at each airport, `min` is the median
  gate-to-gate minutes, `n` is how many flights were seen in the last 35 days,
  and `days` lists ISO weekdays (1 = Monday).
- `meta.json`: the last day processed.

These are observed times (wheels-up and touchdown, adjusted for taxi), not
official schedules. Treat them as "usually", and check the ticket.

## How it works

`.github/workflows/nightly.yml` runs daily. It downloads the previous UTC
day's archive from [adsb.lol globe_history](https://github.com/adsblol/globe_history_2026),
streams it through `pipeline/build.py`, keeps a rolling 35-day state, and
force-pushes the result to the `gh-pages` branch, so there's no history growth.

Run one day by hand from Actions › nightly › Run workflow, with a date.

## Licence

Source data © adsb.lol contributors, [ODbL 1.0](https://opendatacommons.org/licenses/odbl/1-0/).
The published flight times are a derived database under the same licence.
Airport coordinates are from [mwgg/Airports](https://github.com/mwgg/Airports) (MIT).
