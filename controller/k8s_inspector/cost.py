"""What a run's attempts cost and how much of it was wasted, and the log lines that report it.

The definitions are in docs/reference/run-records.md ("Cost log lines"). `add_costs` puts the numbers into a run
record; `log_cost_lines` writes one `run_attempt_cost` line per attempt and one `run_cost` line per run.
"""

import logging
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime

log = logging.getLogger(__name__)

WEEKDAYS = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")


@dataclass(frozen=True)
class Span:
    """A task execution: the attempt it ran in, when it started and ended, and whether its result was kept
    (published) or discarded."""

    attempt: int | None
    start: datetime | None
    end: datetime | None
    kept: bool


def parse_time(text: str | datetime | None) -> datetime | None:
    """An ISO time as UTC; a naive one (the `meta.json` files) is UTC already."""
    if text is None:
        return None
    when = text if isinstance(text, datetime) else datetime.fromisoformat(text)
    return when.replace(tzinfo=UTC) if when.tzinfo is None else when.astimezone(UTC)


def spans_from_metas(attempts: list[dict], metas: Iterable[dict]) -> list[Span]:
    """The kept tasks of a past run, from the `start`/`end` of the `meta.json` files filed under its run id. A
    `meta.json` has no attempt number: the task belongs to the attempt that was open when it started."""
    spans = []
    for meta in metas:
        start, end = parse_time(meta.get("start")), parse_time(meta.get("end"))
        attempt = None
        for a in attempts:
            opened, closed = parse_time(a.get("api_start")), parse_time(a.get("end"))
            if start and opened and opened <= start and (closed is None or start <= closed):
                attempt = a["attempt"]
        spans.append(Span(attempt, start, end, kept=True))
    return spans


def _seconds(start: datetime | None, end: datetime | None) -> float | None:
    return round((end - start).total_seconds(), 3) if start and end else None


def attempt_costs(attempt: dict, spans: list[Span] | None) -> dict:
    """`duration_s`, `useful_s`, `setup_s`, `cost_usd` and `wasted_usd` of one attempt of a run record.

    `spans` are the run's task executions; None when they are not known (a past run without result files):
    `useful_s`, `setup_s` and `wasted_usd` are then null, except for a Completed attempt, which is useful as a
    whole. A `useful_s` the attempt already has is kept. An attempt without a price or an end costs 0."""
    start = parse_time(attempt.get("api_start"))
    duration = _seconds(start, parse_time(attempt.get("end")))
    price = float(attempt["price"]) if attempt.get("price") else None
    mine = [s for s in spans or [] if s.attempt == attempt["attempt"]]
    if "useful_s" in attempt:
        useful = attempt["useful_s"]
    elif attempt.get("outcome") == "Completed":
        useful = duration or 0.0
    elif spans is None:
        useful = None
    else:
        useful = max([_seconds(start, s.end) or 0.0 for s in mine if s.kept] + [0.0])
    starts = [s.start for s in mine if s.start]
    setup = attempt["setup_s"] if "setup_s" in attempt else _seconds(start, min(starts)) if starts else None
    cost = round(duration / 3600 * price, 6) if duration is not None and price is not None else 0.0
    wasted = None if useful is None else round(cost - useful / 3600 * (price or 0.0), 6)
    return {"duration_s": duration, "useful_s": useful, "setup_s": setup, "cost_usd": cost, "wasted_usd": wasted}


def add_costs(run_json: dict, spans: list[Span] | None) -> dict:
    """A copy of the run record (or `run.json`) whose attempts carry the cost fields of `attempt_costs`."""
    record = dict(run_json)
    record["attempts"] = [{**a, **attempt_costs(a, spans)} for a in run_json.get("attempts") or []]
    return record


def _sum(values: Iterable[float | None]) -> float | None:
    values = list(values)
    return None if any(v is None for v in values) else round(sum(values), 6)


def run_summary(record: dict) -> dict:
    """The `run_cost` fields of a record whose attempts have their cost fields.

    The on-demand counterfactual is what the run would have cost on-demand in the region of its Completed
    attempt: all useful time plus that attempt's setup. It is null without such an attempt, without that price, or
    when the useful time of an attempt is unknown. A null `setup_s` counts as 0."""
    attempts = record.get("attempts") or []
    done = next((a for a in attempts if a.get("outcome") == "Completed"), None)
    useful = _sum(a.get("useful_s") for a in attempts)
    counterfactual = None
    if done and done.get("ondemand_price") is not None and useful is not None:
        counterfactual = round(done["ondemand_price"] * (useful + (done.get("setup_s") or 0.0)) / 3600, 6)
    paid = round(sum(a["cost_usd"] for a in attempts), 6)
    last = attempts[-1] if attempts else {}
    return {
        "family": record.get("family"),
        "final_region": last.get("region"),
        "final_capacity_type": last.get("capacity_type"),
        "attempts": len(attempts),
        "spot_interruptions": sum(1 for a in attempts if a.get("outcome") == "SpotInterrupted"),
        "used_spot": any(a.get("capacity_type") == "spot" for a in attempts),
        "paid_usd": paid,
        "wasted_usd": _sum(a.get("wasted_usd") for a in attempts),
        "ondemand_counterfactual_usd": counterfactual,
        "spot_net_usd": None if counterfactual is None else round(paid - counterfactual, 6),
    }


def log_cost_lines(record: dict, *, backfill: bool = False) -> None:
    """One `run_attempt_cost` line per attempt and one `run_cost` line for the run (info, JSON logger). The
    record's attempts must carry the cost fields (`add_costs`). An attempt without `ondemand_price_source` has
    the price of its launch."""
    who = {"run": record["run_id"], "vendor": record["vendor"], "sku": record["sku"]}
    for a in record.get("attempts") or []:
        start = parse_time(a.get("api_start"))
        log.info(
            "run attempt cost",
            extra={
                "event": "run_attempt_cost",
                **who,
                "family": record.get("family"),
                "attempt": a["attempt"],
                "region": a.get("region"),
                "zone": a.get("zone"),
                "capacity_type": a.get("capacity_type"),
                "outcome": a.get("outcome"),
                "start": a.get("api_start"),
                "end": a.get("end"),
                "duration_s": a["duration_s"],
                "useful_s": a["useful_s"],
                "setup_s": a["setup_s"],
                "price": float(a["price"]) if a.get("price") else None,
                "ondemand_price": a.get("ondemand_price"),
                "ondemand_price_source": a.get("ondemand_price_source", "launch"),
                "cost_usd": a["cost_usd"],
                "wasted_usd": a["wasted_usd"],
                "start_hour_utc": start.hour if start else None,
                "start_weekday_utc": WEEKDAYS[start.weekday()] if start else None,
                "backfill": backfill,
            },
        )
    log.info("run cost", extra={"event": "run_cost", **who, **run_summary(record), "backfill": backfill})
