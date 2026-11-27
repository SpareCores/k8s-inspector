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
    """A task execution: the attempt it ran in, when it started and ended, and whether its result was kept (published)
    or discarded. A task the controller wrote off (`-1`, `-4`: start and end of the moment it was written off) has a
    span too; only `finished` ones (exit code 0 or more) count for `attempt_costs`."""

    attempt: int | None
    start: datetime | None
    end: datetime | None
    kept: bool
    task: str = ""
    exit_code: int = 0
    error_msg: str | None = None

    @property
    def finished(self) -> bool:
        return self.exit_code >= 0


def parse_time(text: str | datetime | None) -> datetime | None:
    """An ISO time as UTC; a naive one (the `meta.json` files) is UTC already."""
    if text is None:
        return None
    when = text if isinstance(text, datetime) else datetime.fromisoformat(text)
    return when.replace(tzinfo=UTC) if when.tzinfo is None else when.astimezone(UTC)


def spans_from_metas(attempts: list[dict], metas: Iterable[dict]) -> list[Span]:
    """The tasks of a past run, from the `start`/`end` of the `meta.json` files filed under its run id (the caller adds
    the `task` name). A `meta.json` has no attempt number: the task belongs to the attempt that was open when it
    started."""
    spans = []
    for meta in metas:
        if meta.get("exit_code") is None:
            continue
        start, end = parse_time(meta.get("start")), parse_time(meta.get("end"))
        attempt = None
        for a in attempts:
            opened, closed = parse_time(a.get("api_start")), parse_time(a.get("end"))
            if start and opened and opened <= start and (closed is None or start <= closed):
                attempt = a["attempt"]
        spans.append(Span(attempt, start, end, True, meta.get("task", ""), meta["exit_code"], meta.get("error_msg")))
    return spans


def _seconds(start: datetime | None, end: datetime | None) -> float | None:
    return round((end - start).total_seconds(), 3) if start and end else None


def has_node(attempt: dict) -> bool:
    """Whether the attempt got a node: only then it has a `region` (and `price`)."""
    return bool(attempt.get("region"))


def attempt_costs(attempt: dict, spans: list[Span] | None) -> dict:
    """`duration_s`, `useful_s`, `setup_s`, `task_s`, `cost_usd` and `wasted_usd` of one attempt of a run record.

    `spans` are the run's task executions; None when they are not known (a past run without result files):
    `useful_s`, `setup_s` and `wasted_usd` are then null, except for a Completed attempt, which is useful as a
    whole, and an attempt that ran no task (no node, or `ProvisioningFailed`/`LaunchFailed`): useful 0, so it wasted
    what it cost. A `useful_s` the attempt already has is kept. An attempt without a node, a price or an end costs
    0 and wastes 0."""
    start = parse_time(attempt.get("api_start"))
    duration = _seconds(start, parse_time(attempt.get("end")))
    price = float(attempt["price"]) if attempt.get("price") else None
    mine = [s for s in spans or [] if s.attempt == attempt["attempt"] and s.finished]
    if "useful_s" in attempt:
        useful = attempt["useful_s"]
    elif attempt.get("outcome") == "Completed":
        useful = duration or 0.0
    elif not has_node(attempt) or attempt.get("outcome") in ("ProvisioningFailed", "LaunchFailed"):
        useful = 0.0
    elif spans is None:
        useful = None
    else:
        useful = max([_seconds(start, s.end) or 0.0 for s in mine if s.kept] + [0.0])
    starts = [s.start for s in mine if s.start]
    setup = attempt["setup_s"] if "setup_s" in attempt else _seconds(start, min(starts)) if starts else None
    cost = (
        round(duration / 3600 * price, 6) if duration is not None and price is not None and has_node(attempt) else 0.0
    )
    task = None if useful is None else round(max(useful - (setup or 0.0), 0.0), 3)
    if attempt.get("outcome") == "Completed" or not has_node(attempt):
        wasted = 0.0
    else:
        wasted = None if task is None else round(cost - task / 3600 * (price or 0.0), 6)
    return {
        "duration_s": duration,
        "useful_s": useful,
        "setup_s": setup,
        "task_s": task,
        "cost_usd": cost,
        "wasted_usd": wasted,
    }


def first_try(run_json: dict) -> datetime | None:
    """When the run first tried to get a machine: the earliest of the SKU's first counted spot try
    (`first_spot_try`, set when the run got a node after such tries) and the `api_start` of the run's attempts."""
    times = [parse_time(run_json.get("first_spot_try"))] + [
        parse_time(a.get("api_start")) for a in run_json["attempts"]
    ]
    return min((t for t in times if t), default=None)


def capacity_wait(attempt: dict, first: datetime | None) -> dict:
    """`first_try_at` and `capacity_wait_s` of one attempt: the seconds from `first` until the cloud gave it its
    node, which is when its NodeClaim launched (`api_end`; `machine_start`, then `node_registered` when that is
    missing). Null for an attempt without a node."""
    got = next(
        (parse_time(attempt[k]) for k in ("api_end", "machine_start", "node_registered") if attempt.get(k)), None
    )
    wait = _seconds(first, got) if has_node(attempt) else None
    return {
        "first_try_at": first.isoformat() if first else None,
        "capacity_wait_s": None if wait is None else max(wait, 0.0),
    }


def add_costs(run_json: dict, spans: list[Span] | None) -> dict:
    """A copy of the run record (or `run.json`) whose attempts carry the cost fields of `attempt_costs` and the
    capacity fields of `capacity_wait`."""
    record = dict(run_json)
    first = first_try(run_json) if run_json.get("attempts") else None
    record["attempts"] = [
        {**a, **attempt_costs(a, spans), **capacity_wait(a, first)} for a in run_json.get("attempts") or []
    ]
    return record


def _sum(values: Iterable[float | None]) -> float | None:
    values = list(values)
    return None if any(v is None for v in values) else round(sum(values), 6)


def run_summary(record: dict) -> dict:
    """The `run_cost` fields of a record whose attempts have their cost fields.

    The on-demand counterfactual is what the run would have cost on-demand in the region of its reference attempt:
    all kept task time plus one setup, that attempt's. The reference attempt is the Completed one, else the last
    attempt with kept task time (`task_s` > 0, so it has a `setup_s`; a run that ended otherwise still published the
    results it kept). Its price is its own
    `price` when it is on-demand, else its `ondemand_price`. It is null without a reference attempt, without that
    price, or when the task time of an attempt is unknown. A null `setup_s` counts as 0."""
    attempts = record.get("attempts") or []
    done = next((a for a in attempts if a.get("outcome") == "Completed"), None)
    ref = done or next((a for a in reversed(attempts) if (a.get("task_s") or 0) > 0), None)
    task = _sum(a.get("task_s") for a in attempts)
    counterfactual = None
    price = None
    if ref:
        own = float(ref["price"]) if ref.get("price") else None
        price = own if ref.get("capacity_type") == "on-demand" and own is not None else ref.get("ondemand_price")
    if price is not None and task is not None:
        counterfactual = round(price * (task + (ref.get("setup_s") or 0.0)) / 3600, 6)
    paid = round(sum(a["cost_usd"] for a in attempts), 6)
    last = attempts[-1] if attempts else {}
    return {
        "family": record.get("family"),
        "series": record.get("series"),
        "final_region": last.get("region"),
        "final_capacity_type": last.get("capacity_type"),
        "attempts": len(attempts),
        "spot_interruptions": sum(1 for a in attempts if a.get("outcome") == "SpotInterrupted"),
        "used_spot": any(a.get("capacity_type") == "spot" and has_node(a) for a in attempts),
        "paid_usd": paid,
        "wasted_usd": _sum(a.get("wasted_usd") for a in attempts),
        "ondemand_counterfactual_usd": counterfactual,
        "spot_net_usd": None if counterfactual is None else round(paid - counterfactual, 6),
    }


def task_result(exit_code: int) -> str:
    """`ok` (0), `unfinished` (`-4`: the task never finished, `error_msg` says why: node lost, deadline, cancel, ...)
    or `failed` (the task's own code, `256`, `-6`, ...) (exit-codes.md)."""
    return "ok" if exit_code == 0 else "unfinished" if exit_code == -4 else "failed"


def log_task_cost_lines(record: dict, spans: list[Span], *, backfill: bool = False) -> None:
    """One `task_cost` line per task execution that ran on a node: it has an end after its start and its attempt got
    a node and a price. A task that never started (written off with start and end of the same moment) has none."""
    attempts = {a["attempt"]: a for a in record.get("attempts") or []}
    for s in spans:
        a = attempts.get(s.attempt)
        if not (a and has_node(a) and a.get("price") and s.start and s.end and s.end > s.start):
            continue
        price = float(a["price"])
        duration = _seconds(s.start, s.end)
        log.info(
            "task cost",
            extra={
                "event": "task_cost",
                "run": record["run_id"],
                "attempt": s.attempt,
                "task": s.task,
                "vendor": record["vendor"],
                "sku": record["sku"],
                "family": record.get("family"),
                "series": record.get("series"),
                "region": a["region"],
                "zone": a.get("zone"),
                "capacity_type": a.get("capacity_type"),
                "start": s.start.isoformat(),
                "end": s.end.isoformat(),
                "duration_s": duration,
                "price": price,
                "cost_usd": round(duration / 3600 * price, 6),
                "exit_code": s.exit_code,
                "result": task_result(s.exit_code),
                "error_msg": s.error_msg,
                "backfill": backfill,
            },
        )


def log_cost_lines(record: dict, spans: list[Span] | None = None, *, backfill: bool = False) -> None:
    """One `run_attempt_cost` line per attempt, one `task_cost` line per task (`log_task_cost_lines`) and one
    `run_cost` line for the run (info, JSON logger). The record's attempts must carry the cost fields
    (`add_costs`). An attempt without `ondemand_price_source` has the price of its launch."""
    who = {"run": record["run_id"], "vendor": record["vendor"], "sku": record["sku"]}
    for a in record.get("attempts") or []:
        start = parse_time(a.get("api_start"))
        log.info(
            "run attempt cost",
            extra={
                "event": "run_attempt_cost",
                **who,
                "family": record.get("family"),
                "series": record.get("series"),
                "vcpus": record.get("vcpus"),
                "memory_gib": record.get("memory_gib"),
                "attempt": a["attempt"],
                "node": a.get("node"),
                "nodeclaim": a.get("node_claim"),
                "region": a.get("region"),
                "zone": a.get("zone"),
                "capacity_type": a.get("capacity_type"),
                "outcome": a.get("outcome"),
                "start": a.get("api_start"),
                "end": a.get("end"),
                "first_try_at": a["first_try_at"],
                "capacity_wait_s": a["capacity_wait_s"],
                "duration_s": a["duration_s"],
                "useful_s": a["useful_s"],
                "setup_s": a["setup_s"],
                "task_s": a["task_s"],
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
    log_task_cost_lines(record, spans or [], backfill=backfill)
    log.info("run cost", extra={"event": "run_cost", **who, **run_summary(record), "backfill": backfill})
