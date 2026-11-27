"""`spot/<vendor>.json` of sc-inspector-data: spot interruption and availability statistics per vendor, aggregated
from the run records (`data/<vendor>/<sku>/runs/<run_id>.json`). Definitions and caveats: docs/reference/spot-stats.md.
Pure functions: the publisher reads the records and writes the file."""

import json
import math
from collections import defaultdict
from datetime import datetime

SCHEMA = 1
MIN_ATTEMPTS = 5  # groups with fewer spot attempts give counts only
WAVE_GAP_DAYS = 14  # a gap this long between spot attempts starts a new wave
Z95 = 1.959964  # two-sided 95% normal quantile
METHOD = (
    "k8s-inspector benchmark runs; interruptions per spot-hour (Poisson 95% CI); availability per spot launch "
    f"try (Wilson 95% CI); groups with fewer than {MIN_ATTEMPTS} spot attempts give counts only"
)
LEVELS = {  # level -> the attempt fields of its key
    "vendor": (),
    "region": ("region",),
    "family": ("family",),
    "series": ("series",),
    "region_family": ("region", "family"),
}


def poisson_ci95(events: int, hours: float) -> tuple[float, float]:
    """Exact (Garwood) 95% confidence interval of `events` per `hours`, as a rate per hour."""

    def cdf(k: int, mean: float) -> float:  # P(X <= k)
        term = total = math.exp(-mean)
        for i in range(1, k + 1):
            term *= mean / i
            total += term
        return total

    def solve(target) -> float:  # bisection on the mean for a function falling in it
        low, high = 0.0, events + 10 * math.sqrt(events + 1) + 10
        for _ in range(100):
            mid = (low + high) / 2
            low, high = (mid, high) if target(mid) else (low, mid)
        return (low + high) / 2

    lower = solve(lambda mean: 1 - cdf(events - 1, mean) < 0.025) if events else 0.0  # P(X >= k) rises with mean
    upper = solve(lambda mean: cdf(events, mean) > 0.025)  # P(X <= k) falls with mean
    return lower / hours, upper / hours


def wilson_ci95(successes: int, tries: int) -> tuple[float, float]:
    """Wilson 95% confidence interval of a proportion."""
    p = successes / tries
    denominator = 1 + Z95**2 / tries
    centre = (p + Z95**2 / (2 * tries)) / denominator
    half = Z95 * math.sqrt(p * (1 - p) / tries + Z95**2 / (4 * tries**2)) / denominator
    return max(centre - half, 0.0), min(centre + half, 1.0)


def _time(text: str) -> datetime:
    return datetime.fromisoformat(text.replace("Z", "+00:00"))


def _events(records: list[dict]) -> list[dict]:
    """One dict per spot attempt that got a node or tried to launch one: the grouping fields, `start`, `hours`,
    `interrupted` and `unavailable`."""
    events = []
    for record in records:
        for a in record.get("attempts") or []:
            unavailable = a.get("outcome") == "SpotUnavailable"
            if a.get("capacity_type") != "spot" and not unavailable:
                continue
            if not a.get("api_start") or not (unavailable or a.get("region")):
                continue
            start = _time(a["api_start"])
            hours = 0.0 if unavailable or not a.get("end") else (_time(a["end"]) - start).total_seconds() / 3600
            events.append(
                {
                    "region": a.get("region"),
                    "family": record.get("family"),
                    "series": record.get("series"),
                    "start": start,
                    "hours": max(hours, 0.0),
                    "interrupted": a.get("outcome") == "SpotInterrupted",
                    "unavailable": unavailable,
                }
            )
    return events


def _waves(starts: list[datetime]) -> list[dict]:
    waves: list[list[datetime]] = []
    for start in sorted(starts):
        if waves and (start - waves[-1][-1]).days < WAVE_GAP_DAYS:
            waves[-1].append(start)
        else:
            waves.append([start])
    return [{"from": w[0].date().isoformat(), "to": w[-1].date().isoformat(), "spot_attempts": len(w)} for w in waves]


def _group(level: str, key: dict, events: list[dict], tracked: bool) -> dict:
    spot = [e for e in events if not e["unavailable"]]
    group = {"level": level, "key": key}
    days = [e["start"].date().isoformat() for e in events]
    group |= {"observed_from": min(days), "observed_to": max(days)}
    if level == "vendor":
        group["waves"] = _waves([e["start"] for e in spot])
    hours = sum(e["hours"] for e in spot)
    interruptions = sum(e["interrupted"] for e in spot)
    tries = len(events) if tracked else 0  # an attempt that got a node and one that found none both tried to launch
    unavailable = sum(e["unavailable"] for e in events)
    group |= {
        "spot_attempts": len(spot),
        "spot_hours": round(hours, 1),
        "interruptions": interruptions,
        "interruptions_per_100h": None,
        "launch_tries": tries,
        "launch_unavailable": unavailable,
        "availability": None,
    }
    if len(spot) >= MIN_ATTEMPTS and hours > 0:
        low, high = poisson_ci95(interruptions, hours)
        group["interruptions_per_100h"] = {
            "value": round(interruptions / hours * 100, 2),
            "ci95": [round(low * 100, 2), round(high * 100, 2)],
        }
    if tries >= MIN_ATTEMPTS:
        low, high = wilson_ci95(tries - unavailable, tries)
        group["availability"] = {
            "value": round((tries - unavailable) / tries, 3),
            "ci95": [round(low, 3), round(high, 3)],
        }
    return group


def spot_stats(vendor: str, records: list[dict], generated_at: str) -> dict | None:
    """The `spot/<vendor>.json` document of the server run records of `vendor`; None without any spot attempt."""
    events = _events(records)
    if not any(not e["unavailable"] for e in events):
        return None
    tracked = any(e["unavailable"] for e in events)  # SpotUnavailable attempts exist: launch tries are known
    buckets: dict[tuple, list[dict]] = defaultdict(list)
    for e in events:
        for level, fields in LEVELS.items():
            if all(e[f] for f in fields):  # records without family or series (older ones) only count where known
                buckets[(level, tuple((f, e[f]) for f in fields))].append(e)
    groups = [_group(level, dict(key), items, tracked) for (level, key), items in buckets.items()]
    order = list(LEVELS)
    groups.sort(key=lambda g: (order.index(g["level"]), sorted(g["key"].items())))
    return {"schema": SCHEMA, "vendor": vendor, "generated_at": generated_at, "method": METHOD, "groups": groups}


def render(stats: dict) -> str:
    return json.dumps(stats, indent=2) + "\n"


def same_content(old: str, new: dict) -> bool:
    """`old` (the file's text) holds `new` apart from `generated_at`."""
    try:
        return {**json.loads(old), "generated_at": None} == {**new, "generated_at": None}
    except ValueError:
        return False
