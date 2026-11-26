"""Cost math and cost log lines: the c6a.12xlarge run of 2026-11-26 (two interrupted spot attempts that kept some
tasks, then on-demand)."""

import json
import logging
from datetime import UTC, datetime

import pytest

from k8s_inspector import cost


def t(text: str) -> datetime:
    return datetime.fromisoformat(text).replace(tzinfo=UTC)


def attempt(n, start, end, outcome, capacity, price, region, ondemand=None, **extra):
    return {
        "attempt": n,
        "api_start": start,
        "end": end,
        "outcome": outcome,
        "capacity_type": capacity,
        "price": price,
        "region": region,
        "zone": region + "a",
        "ondemand_price": ondemand,
    } | extra


def c6a(ondemand_final=1.122) -> dict:
    return {
        "run_id": "r20261126t0813-cd26",
        "vendor": "aws",
        "sku": "c6a.12xlarge",
        "family": "Compute Optimized",
        "series": "c6a",
        "attempts": [
            attempt(
                1, "2026-11-26T08:13:43Z", "2026-11-26T08:31:55+00:00", "SpotInterrupted", "spot", "0.36", "r1", 2.0
            ),
            attempt(
                2, "2026-11-26T08:32:00Z", "2026-11-26T09:12:48+00:00", "SpotInterrupted", "spot", "0.18", "r2", 1.0
            ),
            attempt(
                3,
                "2026-11-26T09:13:00Z",
                "2026-11-26T10:56:48+00:00",
                "Completed",
                "on-demand",
                "1.2",
                "r3",
                ondemand_final,
            ),
        ],
    }


SPANS = [
    cost.Span(1, t("2026-11-26T08:15:43"), t("2026-11-26T08:16:43"), kept=True),
    cost.Span(1, t("2026-11-26T08:16:43"), t("2026-11-26T08:17:43"), kept=False),  # discarded: not useful
    cost.Span(2, t("2026-11-26T08:34:00"), t("2026-11-26T09:00:00"), kept=True),
    cost.Span(3, t("2026-11-26T09:15:00"), t("2026-11-26T10:56:00"), kept=True),
]


def test_attempt_costs_of_the_interrupted_and_the_completed_attempts():
    a1, a2, a3 = cost.add_costs(c6a(), SPANS)["attempts"]
    assert (a1["duration_s"], a1["useful_s"], a1["setup_s"]) == (1092.0, 180.0, 120.0)  # last kept task ends 08:16:43
    assert a1["cost_usd"] == pytest.approx(1092 / 3600 * 0.36)
    assert a1["task_s"] == 60.0  # useful time without the setup
    assert a1["wasted_usd"] == pytest.approx((1092 - 60) / 3600 * 0.36)  # setup and the tail count as waste
    assert (a2["useful_s"], a2["setup_s"], a2["task_s"]) == (28 * 60.0, 120.0, 1560.0)
    assert (a3["useful_s"], a3["duration_s"], a3["task_s"], a3["wasted_usd"]) == (6228.0, 6228.0, 6108.0, 0.0)


def test_an_attempt_without_a_kept_task_wastes_everything_and_one_without_a_node_costs_nothing():
    record = {
        "attempts": [
            attempt(1, "2026-11-26T08:00:00Z", "2026-11-26T09:00:00+00:00", "SpotInterrupted", "spot", "1.0", "r1"),
            {
                "attempt": 2,
                "api_start": "2026-11-26T09:00:00Z",
                "end": "2026-11-26T09:05:00+00:00",
                "outcome": "ProvisioningFailed",
            },
        ]
    }
    a1, a2 = cost.add_costs(record, [cost.Span(1, t("2026-11-26T08:10:00"), t("2026-11-26T08:20:00"), kept=False)])[
        "attempts"
    ]
    assert (a1["useful_s"], a1["cost_usd"], a1["wasted_usd"]) == (0.0, 1.0, 1.0)
    assert a1["setup_s"] == 600.0  # the first task started, kept or not
    assert (a2["cost_usd"], a2["useful_s"], a2["wasted_usd"], a2["setup_s"]) == (0.0, 0.0, 0.0, None)


def test_run_summary_with_spot_interruptions_then_on_demand():
    summary = cost.run_summary(cost.add_costs(c6a(), SPANS))
    paid = (1092 * 0.36 + 2448 * 0.18 + 6228 * 1.2) / 3600
    tasks = 60 + 1560 + 6108
    assert summary["paid_usd"] == pytest.approx(paid, abs=1e-5)
    # counterfactual: the completed attempt's price x (all task time + its setup, 120 s); an on-demand Completed
    # attempt uses its own price (1.2), not Keeper's list price
    assert summary["ondemand_counterfactual_usd"] == pytest.approx(1.2 * (tasks + 120) / 3600, abs=1e-5)
    assert summary["spot_net_usd"] == pytest.approx(paid - 1.2 * (tasks + 120) / 3600, abs=1e-5)
    assert summary | {"paid_usd": 0, "wasted_usd": 0, "ondemand_counterfactual_usd": 0, "spot_net_usd": 0} == {
        "family": "Compute Optimized",
        "series": "c6a",
        "final_region": "r3",
        "final_capacity_type": "on-demand",
        "attempts": 3,
        "spot_interruptions": 2,
        "used_spot": True,
        "paid_usd": 0,
        "wasted_usd": 0,
        "ondemand_counterfactual_usd": 0,
        "spot_net_usd": 0,
    }


def test_counterfactual_is_null_without_the_ondemand_price():
    record = c6a(ondemand_final=None)
    record["attempts"][2] |= {"capacity_type": "spot", "price": "0.5"}  # a spot Completed attempt needs the list price
    summary = cost.run_summary(cost.add_costs(record, SPANS))
    assert summary["ondemand_counterfactual_usd"] is None and summary["spot_net_usd"] is None
    assert summary["paid_usd"] > 0
    record["attempts"][2]["ondemand_price"] = 1.0
    summary = cost.run_summary(cost.add_costs(record, SPANS))
    assert summary["ondemand_counterfactual_usd"] == pytest.approx(1.0 * (60 + 1560 + 6108 + 120) / 3600, abs=1e-5)


def test_counterfactual_is_null_without_a_completed_attempt():
    record = c6a()
    record["attempts"] = record["attempts"][:2]
    summary = cost.run_summary(cost.add_costs(record, SPANS))
    assert summary["ondemand_counterfactual_usd"] is None and summary["spot_net_usd"] is None
    assert summary["spot_interruptions"] == 2 and summary["used_spot"] is True


def test_unknown_task_times_leave_useful_time_and_waste_null():
    a1, _, a3 = cost.add_costs(c6a(), None)["attempts"]
    assert (a1["useful_s"], a1["setup_s"], a1["wasted_usd"]) == (None, None, None)
    assert a3["useful_s"] == 6228.0
    summary = cost.run_summary(cost.add_costs(c6a(), None))
    assert summary["wasted_usd"] is None and summary["ondemand_counterfactual_usd"] is None


def test_spans_from_metas_assign_a_task_to_the_attempt_open_when_it_started():
    attempts = cost.add_costs(c6a(), SPANS)["attempts"]
    metas = [
        {"start": "2026-11-26T08:15:43.5", "end": "2026-11-26T08:16:43", "exit_code": 0},
        {"start": "2026-11-26T09:15:00", "end": "2026-11-26T10:56:00", "exit_code": 1},
    ]
    assert [s.attempt for s in cost.spans_from_metas(attempts, metas)] == [1, 3]


def test_log_lines_carry_the_documented_fields(caplog):
    record = cost.add_costs(c6a(), SPANS)
    with caplog.at_level(logging.INFO, logger="k8s_inspector.cost"):
        cost.log_cost_lines(record)
    lines = [r.__dict__ for r in caplog.records]
    assert [(r["event"], r["msg"]) for r in lines] == [
        ("run_attempt_cost", "run attempt cost"),
        ("run_attempt_cost", "run attempt cost"),
        ("run_attempt_cost", "run attempt cost"),
        ("run_cost", "run cost"),
    ]
    attempt_fields = {
        "run", "vendor", "sku", "family", "series", "region", "zone", "capacity_type", "outcome", "start", "end",
        "duration_s",
        "useful_s", "setup_s", "task_s", "price", "ondemand_price", "cost_usd", "wasted_usd", "start_hour_utc",
        "start_weekday_utc",
    }  # fmt: skip
    run_fields = {
        "run", "vendor", "sku", "family", "series", "final_region", "final_capacity_type", "attempts",
        "spot_interruptions",
        "used_spot", "paid_usd", "wasted_usd", "ondemand_counterfactual_usd", "spot_net_usd",
    }  # fmt: skip
    assert attempt_fields <= lines[0].keys() and run_fields <= lines[3].keys()
    assert (lines[0]["start_hour_utc"], lines[0]["start_weekday_utc"]) == (8, "Thu")
    assert (lines[0]["ondemand_price_source"], lines[0]["backfill"]) == ("launch", False)
    json.dumps({k: v for k, v in lines[3].items() if k in run_fields})


def test_backfill_lines_say_so(caplog):
    with caplog.at_level(logging.INFO, logger="k8s_inspector.cost"):
        cost.log_cost_lines(cost.add_costs(c6a(), SPANS), backfill=True)
    assert all(r.backfill is True for r in caplog.records)


def test_tasks_the_controller_wrote_off_do_not_count_as_kept():
    """An interrupted last attempt: `-4` tasks have start and end of the moment the node was lost."""
    attempts = [attempt(1, "2026-11-26T08:00:00Z", "2026-11-26T09:00:00+00:00", "SpotInterrupted", "spot", "1.0", "r1")]
    metas = [
        {"start": "2026-11-26T08:05:00", "end": "2026-11-26T08:10:00", "exit_code": 0},
        {"start": "2026-11-26T09:00:00", "end": "2026-11-26T09:00:00", "exit_code": -4},
        {"start": "2026-11-26T09:00:00", "end": "2026-11-26T09:00:00", "exit_code": -1},
    ]
    spans = cost.spans_from_metas(attempts, metas)
    assert len(spans) == 1
    a = cost.add_costs({"attempts": attempts}, spans)["attempts"][0]
    assert (a["useful_s"], a["setup_s"], a["task_s"]) == (600.0, 300.0, 300.0)
    assert a["wasted_usd"] == pytest.approx((3600 - 300) / 3600)


REAL_C6A = {  # the run of 2026-11-26, as its record holds it
    "run_id": "r20261126t0813-cd26", "vendor": "aws", "sku": "c6a.12xlarge",
    "family": "Compute Optimized", "series": "c6a",
    "attempts": [
        attempt(1, "2026-11-26T08:13:43Z", "2026-11-26T08:31:55.104671+00:00", "SpotInterrupted", "spot",
                "0.31903013698630134", "ca-central-1", 2.0088),
        attempt(2, "2026-11-26T08:32:01Z", "2026-11-26T09:12:49.741619+00:00", "SpotInterrupted", "spot",
                "0.13319123287671233", "ap-south-2", 1.122),
        attempt(3, "2026-11-26T09:13:00Z", "2026-11-26T10:56:47.633642+00:00", "Completed", "on-demand",
                "1.1429912328767124", "ap-south-1", 1.122),
    ],
}  # fmt: skip
REAL_C6A_TASKS = [  # attempt, start, end of its tasks
    (3, "09:14:54.009212", "09:14:54.045593"), (1, "08:16:02.376148", "08:16:02.387414"),
    (1, "08:16:08.226722", "08:16:08.233131"), (1, "08:16:22.940745", "08:16:22.947040"),
    (2, "08:34:19.364428", "08:59:42.621554"), (3, "09:15:09.075035", "09:30:00.900313"),
    (3, "09:30:15.576205", "09:38:28.684566"), (3, "09:43:57.308372", "10:43:54.671207"),
    (3, "10:44:12.028946", "10:56:39.344049"), (3, "09:14:53.929942", "09:14:53.954036"),
    (3, "09:39:36.725695", "09:39:45.436780"), (3, "09:41:29.241684", "09:43:47.698939"),
]  # fmt: skip


def test_the_real_c6a_run():
    spans = [
        cost.Span(n, t(f"2026-11-26T{start}"), t(f"2026-11-26T{end}"), kept=True) for n, start, end in REAL_C6A_TASKS
    ]
    record = cost.add_costs(REAL_C6A, spans)
    a1, a2, a3 = record["attempts"]
    assert (a1["task_s"], a2["task_s"], a3["task_s"]) == (20.571, 1523.258, 6113.704)
    assert (a1["wasted_usd"], a2["wasted_usd"], a3["wasted_usd"]) == (0.094959, 0.03424, 0.0)
    summary = cost.run_summary(record)
    assert summary["paid_usd"] == pytest.approx(2.1646, abs=1e-4)
    assert summary["wasted_usd"] == pytest.approx(0.1292, abs=1e-4)
    # the on-demand attempt 3 counts with its own price 1.143, not Keeper's list price 1.122
    assert summary["ondemand_counterfactual_usd"] == pytest.approx(2.4674, abs=1e-4)
    assert summary["spot_net_usd"] == pytest.approx(-0.3028, abs=1e-4)


def test_an_attempt_without_a_node_costs_nothing_and_does_not_set_the_final_region():
    record = {
        "attempts": [
            attempt(1, "2026-11-26T08:00:00Z", "2026-11-26T09:00:00+00:00", "SpotInterrupted", "spot", "1.0", "r1"),
            {"attempt": 2, "api_start": "2026-11-26T09:00:00Z", "end": "2026-11-26T09:10:00+00:00",
             "outcome": "ProvisioningFailed", "capacity_type": None, "node": None},
        ]
    }  # fmt: skip
    summary = cost.run_summary(cost.add_costs(record, []))
    assert summary["final_region"] is None and summary["paid_usd"] == 1.0
