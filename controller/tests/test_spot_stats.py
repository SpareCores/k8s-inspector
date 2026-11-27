"""spot/<vendor>.json: the confidence intervals and the aggregation of run records."""

import json

import pytest

from k8s_inspector import spot_stats


def attempt(day, hours, outcome="Completed", region="r1", capacity="spot"):
    start = f"2026-{day}T10:00:00Z"
    h, m = divmod(round(hours * 60), 60)
    end = f"2026-{day}T{10 + h:02d}:{m:02d}:00+00:00"
    return {"api_start": start, "end": end, "outcome": outcome, "capacity_type": capacity, "region": region}


def record(*attempts, family="Compute Optimized", series="c6a"):
    return {"vendor": "aws", "sku": "x", "family": family, "series": series, "attempts": list(attempts)}


def group(stats, level, **key):
    return next(g for g in stats["groups"] if g["level"] == level and g["key"] == key)


def test_poisson_ci_matches_the_exact_values():
    low, high = spot_stats.poisson_ci95(10, 1)
    assert (low, high) == (pytest.approx(4.795, abs=1e-3), pytest.approx(18.390, abs=1e-3))
    low, high = spot_stats.poisson_ci95(0, 2)
    assert low == 0 and high == pytest.approx(3.689 / 2, abs=1e-3)
    assert spot_stats.poisson_ci95(1, 1) == (pytest.approx(0.0253, abs=1e-3), pytest.approx(5.572, abs=1e-3))


def test_wilson_ci_matches_the_known_values():
    low, high = spot_stats.wilson_ci95(8, 10)
    assert (low, high) == (pytest.approx(0.4902, abs=1e-3), pytest.approx(0.9433, abs=1e-3))
    assert spot_stats.wilson_ci95(10, 10)[1] == pytest.approx(1.0)


def test_aggregates_spot_attempts_hours_and_interruptions_per_level():
    records = [
        record(*[attempt("11-01", 2) for _ in range(4)], attempt("11-02", 1, "SpotInterrupted")),
        record(attempt("11-03", 1, "NodeLost", region="r2"), attempt("11-03", 1, capacity="on-demand"), family="GPU"),
    ]
    stats = spot_stats.spot_stats("aws", records, "T")
    vendor = group(stats, "vendor")
    assert (vendor["spot_attempts"], vendor["spot_hours"], vendor["interruptions"]) == (6, 10.0, 1)  # NodeLost no
    assert vendor["interruptions_per_100h"]["value"] == 10.0
    assert (vendor["observed_from"], vendor["observed_to"]) == ("2026-11-01", "2026-11-03")
    assert group(stats, "region", region="r1")["spot_attempts"] == 5
    assert group(stats, "family", family="GPU")["spot_attempts"] == 1
    assert group(stats, "region_family", region="r1", family="Compute Optimized")["spot_attempts"] == 5
    assert group(stats, "series", series="c6a")["interruptions"] == 1
    assert (vendor["launch_tries"], vendor["availability"]) == (0, None)


def test_small_groups_give_counts_only():
    records = [record(*[attempt("11-01", 1) for _ in range(4)])]
    small = group(spot_stats.spot_stats("aws", records, "T"), "vendor")
    assert small["spot_attempts"] == 4 and small["interruptions_per_100h"] is None
    records = [record(*[attempt("11-01", 1) for _ in range(5)])]
    assert group(spot_stats.spot_stats("aws", records, "T"), "vendor")["interruptions_per_100h"]["value"] == 0.0


def test_waves_split_at_gaps_of_14_days_or_more():
    days = ["10-01", "10-14", "10-28", "11-20"]  # gaps of 13, 14 and 23 days
    stats = spot_stats.spot_stats("aws", [record(*[attempt(d, 1) for d in days])], "T")
    assert group(stats, "vendor")["waves"] == [
        {"from": "2026-10-01", "to": "2026-10-14", "spot_attempts": 2},
        {"from": "2026-10-28", "to": "2026-10-28", "spot_attempts": 1},
        {"from": "2026-11-20", "to": "2026-11-20", "spot_attempts": 1},
    ]


def test_spot_unavailable_attempts_count_as_launch_tries():
    unavailable = {"api_start": "2026-11-01T10:00:00Z", "end": "2026-11-01T10:01:00Z", "outcome": "SpotUnavailable",
                   "capacity_type": "spot"}  # fmt: skip
    records = [record(*[attempt("11-01", 1) for _ in range(4)], unavailable, unavailable)]
    vendor = group(spot_stats.spot_stats("aws", records, "T"), "vendor")
    assert (vendor["spot_attempts"], vendor["launch_tries"], vendor["launch_unavailable"]) == (4, 6, 2)
    assert vendor["availability"]["value"] == pytest.approx(0.667, abs=1e-3)
    assert vendor["interruptions_per_100h"] is None  # four spot attempts
    assert group(spot_stats.spot_stats("aws", records, "T"), "family", family="Compute Optimized")["launch_tries"] == 6


def test_no_spot_attempt_gives_no_document_and_records_without_family_count_at_vendor_level():
    assert spot_stats.spot_stats("aws", [record(attempt("11-01", 1, capacity="on-demand"))], "T") is None
    old = {"vendor": "aws", "attempts": [attempt("11-01", 1)]}
    stats = spot_stats.spot_stats("aws", [old], "T")
    assert [g["level"] for g in stats["groups"]] == ["vendor", "region"]


def test_same_content_ignores_generated_at():
    stats = spot_stats.spot_stats("aws", [record(attempt("11-01", 1))], "T1")
    assert spot_stats.same_content(spot_stats.render(stats), {**stats, "generated_at": "T2"})
    assert not spot_stats.same_content(spot_stats.render(stats), {**stats, "vendor": "gcp"})
    assert not spot_stats.same_content("not json", stats)
    assert json.loads(spot_stats.render(stats))["schema"] == 1
