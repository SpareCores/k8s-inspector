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


def test_counterfactual_without_a_completed_attempt_prices_the_kept_work_of_the_last_node_attempt():
    record = c6a()
    record["attempts"] = record["attempts"][:2]  # both spot, interrupted; the kept results were published
    summary = cost.run_summary(cost.add_costs(record, SPANS))
    expected = 1.0 * (60 + 1560 + 120) / 3600  # attempt 2's list price x (kept task time + attempt 2's setup)
    assert summary["ondemand_counterfactual_usd"] == pytest.approx(expected, abs=1e-5)
    assert summary["spot_net_usd"] == pytest.approx(summary["paid_usd"] - expected, abs=1e-5)
    assert summary["spot_interruptions"] == 2 and summary["used_spot"] is True
    record["attempts"][1]["ondemand_price"] = None  # no price: null
    summary = cost.run_summary(cost.add_costs(record, SPANS))
    assert summary["ondemand_counterfactual_usd"] is None and summary["spot_net_usd"] is None


def test_counterfactual_is_null_when_no_task_was_kept():
    lost = attempt(1, "2026-11-26T08:00:00Z", "2026-11-26T09:00:00+00:00", "NodeLost", "spot", "1.0", "r1", 2.0)
    summary = cost.run_summary(cost.add_costs({"attempts": [lost]}, []))
    assert summary["paid_usd"] == 1.0 and summary["wasted_usd"] == 1.0
    assert summary["ondemand_counterfactual_usd"] is None and summary["spot_net_usd"] is None  # not 0


def test_counterfactual_references_the_last_attempt_with_kept_work_not_a_lost_last_one():
    first = attempt(1, "2026-11-26T08:00:00Z", "2026-11-26T09:00:00+00:00", "SpotInterrupted", "spot", "1.0", "r1", 2.0)
    lost = attempt(2, "2026-11-26T09:00:00Z", "2026-11-26T10:00:00+00:00", "NodeLost", "spot", "1.0", "r2", 3.0)
    spans = [cost.Span(1, t("2026-11-26T08:10:00"), t("2026-11-26T08:30:00"), kept=True)]
    a1, a2 = (a for a in cost.add_costs({"attempts": [first, lost]}, spans)["attempts"])
    assert a2["setup_s"] is None and a2["task_s"] == 0.0  # no finished task: no setup either
    summary = cost.run_summary({"attempts": [a1, a2]})
    # attempt 1's list price x (its 1200 s of task time + its 600 s setup)
    assert summary["ondemand_counterfactual_usd"] == pytest.approx(2.0 * 1800 / 3600, abs=1e-5)


def test_counterfactual_is_null_without_a_node_attempt():
    record = {"attempts": [{"attempt": 1, "api_start": "2026-11-26T08:00:00Z", "outcome": "ProvisioningFailed"}]}
    summary = cost.run_summary(cost.add_costs(record, []))
    assert summary["ondemand_counterfactual_usd"] is None and summary["spot_net_usd"] is None


def test_used_spot_needs_a_spot_attempt_that_got_a_node():
    never = {"attempt": 1, "api_start": "2026-11-26T08:00:00Z", "end": "2026-11-26T08:05:00+00:00",
             "outcome": "LaunchFailed", "capacity_type": "spot"}  # fmt: skip
    record = {"attempts": [never, attempt(2, "2026-11-26T08:05:00Z", "2026-11-26T09:00:00+00:00", "Completed",
                                          "on-demand", "1.0", "r1", 1.0, useful_s=3300.0, setup_s=0.0)]}  # fmt: skip
    assert cost.run_summary(cost.add_costs(record, []))["used_spot"] is False
    record["attempts"][1] |= {"capacity_type": "spot"}
    assert cost.run_summary(cost.add_costs(record, []))["used_spot"] is True


@pytest.mark.parametrize("spans", [None, []])  # results replaced (backfill) or known: the same answer
def test_failed_launches_and_attempts_without_a_node_waste_what_they_cost(spans):
    failed = {"attempt": 1, "api_start": "2026-11-26T08:00:00Z", "end": "2026-11-26T08:10:00+00:00",
              "outcome": "ProvisioningFailed", "capacity_type": "spot"}  # fmt: skip
    launch_failed = attempt(2, "2026-11-26T08:10:00Z", "2026-11-26T08:40:00+00:00", "LaunchFailed", "spot", "2.0", "r1")
    a1, a2 = cost.add_costs({"attempts": [failed, launch_failed]}, spans)["attempts"]
    assert (a1["cost_usd"], a1["useful_s"], a1["task_s"], a1["wasted_usd"]) == (0.0, 0.0, 0.0, 0.0)
    assert (a2["useful_s"], a2["wasted_usd"]) == (0.0, a2["cost_usd"]) and a2["cost_usd"] == pytest.approx(1.0)
    summary = cost.run_summary({"attempts": [a1, a2]})
    assert summary["wasted_usd"] == pytest.approx(1.0)


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
        "run", "vendor", "sku", "family", "series", "vcpus", "memory_gib", "node", "nodeclaim", "region", "zone",
        "capacity_type", "outcome", "start", "end", "duration_s",
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


def test_a_first_try_launch_waits_only_for_its_own_node():
    launched = attempt(1, "2026-11-26T08:00:00Z", "2026-11-26T09:00:00+00:00", "Completed", "on-demand", "1.0", "r1",
                       api_end="2026-11-26T08:01:30Z", machine_start="2026-11-26T08:02:00Z")  # fmt: skip
    (a,) = cost.add_costs({"attempts": [launched]}, [])["attempts"]
    assert (a["first_try_at"], a["capacity_wait_s"]) == ("2026-11-26T08:00:00+00:00", 90.0)


def test_a_launch_after_failed_attempts_and_spot_tries_waits_from_the_first_try():
    failed = {"attempt": 1, "api_start": "2026-11-26T08:00:00Z", "end": "2026-11-26T08:10:00+00:00",
              "outcome": "LaunchFailed", "capacity_type": "spot"}  # fmt: skip
    launched = attempt(2, "2026-11-26T08:10:00Z", "2026-11-26T09:00:00+00:00", "Completed", "on-demand", "1.0", "r1",
                       machine_start="2026-11-26T08:12:00Z")  # no api_end: machine_start counts  # fmt: skip
    record = {"attempts": [failed, launched]}
    a1, a2 = cost.add_costs(record, [])["attempts"]
    assert (a1["capacity_wait_s"], a2["capacity_wait_s"]) == (None, 720.0)
    # the SKU's counted spot tries began two days before the run's own first attempt
    a1, a2 = cost.add_costs(record | {"first_spot_try": "2026-11-24T08:00:00+00:00"}, [])["attempts"]
    assert a2["first_try_at"] == "2026-11-24T08:00:00+00:00" and a2["capacity_wait_s"] == 2 * 86400 + 720.0


def test_an_attempt_without_a_node_has_a_first_try_but_no_wait():
    only = {"attempt": 1, "api_start": "2026-11-26T08:00:00Z", "end": "2026-11-26T08:05:00+00:00",
            "outcome": "ProvisioningFailed", "capacity_type": None}  # fmt: skip
    (a,) = cost.add_costs({"attempts": [only]}, [])["attempts"]
    assert (a["first_try_at"], a["capacity_wait_s"]) == ("2026-11-26T08:00:00+00:00", None)


def test_the_attempt_line_carries_first_try_and_capacity_wait(caplog):
    run = c6a() | {"first_spot_try": "2026-11-26T08:00:00+00:00"}
    run["attempts"][2]["api_end"] = "2026-11-26T09:13:30Z"
    record = cost.add_costs(run, SPANS)
    with caplog.at_level(logging.INFO, logger="k8s_inspector.cost"):
        cost.log_cost_lines(record)
    lines = [r.__dict__ for r in caplog.records if r.event == "run_attempt_cost"]
    assert {line["first_try_at"] for line in lines} == {"2026-11-26T08:00:00+00:00"}
    assert [line["capacity_wait_s"] for line in lines] == [None, None, 4410.0]  # only attempt 3 has a launch time


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
    assert [s.finished for s in spans] == [True, False, False]
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
    assert summary["paid_usd"] == pytest.approx(2.164638, abs=1e-6)
    assert summary["wasted_usd"] == pytest.approx(0.1292, abs=1e-4)
    # the on-demand attempt 3 counts with its own price 1.143, not Keeper's list price 1.122
    assert summary["ondemand_counterfactual_usd"] == pytest.approx(2.467421, abs=1e-6)
    assert summary["spot_net_usd"] == pytest.approx(-0.302783, abs=1e-6)


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


def task_lines(caplog) -> list[dict]:
    return [r.__dict__ for r in caplog.records if r.__dict__.get("event") == "task_cost"]


def test_task_cost_is_the_task_duration_times_the_node_price(caplog):
    record = cost.add_costs(REAL_C6A, [])
    spans = [
        cost.Span(3, t("2026-11-26T09:43:57"), t("2026-11-26T10:43:57"), True, "membench", 0),  # one hour
        cost.Span(1, t("2026-11-26T08:20:00"), t("2026-11-26T08:30:00"), False, "geekbench", 1),
        cost.Span(1, t("2026-11-26T08:30:00"), t("2026-11-26T08:31:00"), False, "stress_ng", -4, "DEADLINE_EXCEEDED"),
        cost.Span(1, t("2026-11-26T08:31:55"), t("2026-11-26T08:31:55"), False, "never_ran", -4),  # written off
        cost.Span(2, t("2026-11-26T08:40:00"), t("2026-11-26T08:50:00"), False, "slow", -6),
    ]
    with caplog.at_level(logging.INFO, logger="k8s_inspector.cost"):
        cost.log_cost_lines(record, spans)
    lines = task_lines(caplog)
    assert [(x["task"], x["attempt"], x["result"], x["exit_code"]) for x in lines] == [
        ("membench", 3, "ok", 0), ("geekbench", 1, "failed", 1), ("stress_ng", 1, "unfinished", -4),
        ("slow", 2, "failed", -6),
    ]  # fmt: skip
    membench, geekbench, lost, _ = lines
    assert (membench["duration_s"], membench["price"], membench["cost_usd"]) == (3600.0, 1.1429912328767124, 1.142991)
    assert (membench["vendor"], membench["sku"], membench["region"], membench["capacity_type"]) == (
        "aws", "c6a.12xlarge", "ap-south-1", "on-demand",
    )  # fmt: skip
    assert geekbench["cost_usd"] == pytest.approx(600 / 3600 * 0.31903013698630134, abs=1e-6)
    assert lost["capacity_type"] == "spot" and lost["zone"] == "ca-central-1a"
    assert (lost["error_msg"], membench["error_msg"]) == ("DEADLINE_EXCEEDED", None)
    assert all(x["backfill"] is False for x in lines)


def test_parallel_tasks_each_get_the_full_node_price(caplog):
    record = cost.add_costs(REAL_C6A, [])
    start, end = t("2026-11-26T09:20:00"), t("2026-11-26T09:30:00")
    spans = [cost.Span(3, start, end, True, name, 0) for name in ("dmidecode", "lscpu")]
    with caplog.at_level(logging.INFO, logger="k8s_inspector.cost"):
        cost.log_cost_lines(record, spans)
    one, two = task_lines(caplog)
    assert one["cost_usd"] == two["cost_usd"] == pytest.approx(600 / 3600 * 1.1429912328767124, abs=1e-6)
    assert one["cost_usd"] + two["cost_usd"] > 600 / 3600 * 1.1429912328767124  # they overlap


def test_a_task_of_an_attempt_without_a_node_has_no_line(caplog):
    record = cost.add_costs({"run_id": "r", "vendor": "v", "sku": "s", "attempts": [
        {"attempt": 1, "api_start": "2026-11-26T08:00:00Z", "end": "2026-11-26T08:10:00+00:00", "node": None},
    ]}, [])  # fmt: skip
    with caplog.at_level(logging.INFO, logger="k8s_inspector.cost"):
        cost.log_cost_lines(record, [cost.Span(1, t("2026-11-26T08:01:00"), t("2026-11-26T08:02:00"), True, "a", 0)])
    assert task_lines(caplog) == []


def test_the_task_spans_of_a_real_run_do_not_change_its_cost():
    spans = [cost.Span(n, t(f"2026-11-26T{a}"), t(f"2026-11-26T{b}"), True, "x", 0) for n, a, b in REAL_C6A_TASKS]
    spans.append(cost.Span(1, t("2026-11-26T08:31:55"), t("2026-11-26T08:31:55"), False, "lost", -4))
    summary = cost.run_summary(cost.add_costs(REAL_C6A, spans))
    assert summary["paid_usd"] == pytest.approx(2.164638, abs=1e-6)
    assert summary["ondemand_counterfactual_usd"] == pytest.approx(2.467421, abs=1e-6)


def test_node_fields_and_server_size_are_in_the_attempt_lines(caplog):
    record = c6a() | {"vcpus": 48, "memory_gib": 96.0}
    record["attempts"][0] |= {"node": "ip-10-0-0-1", "node_claim": "pool-abcde"}
    with caplog.at_level(logging.INFO, logger="k8s_inspector.cost"):
        cost.log_cost_lines(cost.add_costs(record, SPANS))
    first, second = [r.__dict__ for r in caplog.records][:2]
    assert (first["node"], first["nodeclaim"], first["vcpus"], first["memory_gib"]) == (
        "ip-10-0-0-1",
        "pool-abcde",
        48,
        96.0,
    )
    assert (second["node"], second["nodeclaim"]) == (None, None)
    caplog.clear()
    with caplog.at_level(logging.INFO, logger="k8s_inspector.cost"):
        cost.log_cost_lines(cost.add_costs(c6a(), SPANS))
    assert (caplog.records[0].vcpus, caplog.records[0].memory_gib) == (None, None)
