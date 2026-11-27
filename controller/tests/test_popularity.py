import zlib

import pytest

from k8s_inspector import keeper, popularity
from k8s_inspector.model import EnvConfig

ENV = EnvConfig(bucket="b", bucket_region="r")


def market(series="s", regions=3, price=1.0, generation="DDR4", accelerators=0, vcpus=4, memory_gib=16):
    return {
        "series": series,
        "accelerators": accelerators,
        "memory_generation": generation,
        "vcpus": vcpus,
        "memory_gib": memory_gib,
        "ondemand": {f"r{i}": price for i in range(regions)},
        "spot": {},
    }


def order(markets, started=(), skus=None):
    skus = skus or list(markets)
    scores = popularity.rank("v", skus, markets, set(started))
    return sorted(skus, key=lambda sku: scores[sku].key)


def test_wider_series_first_and_accelerators_last():
    markets = {
        "gpu": market("g", regions=20, accelerators=1),
        "narrow": market("n", regions=5),
        "wide": market("w", regions=9),
        "other": market("o", regions=5),  # the priced regions of the vendor are many: breadth ranks
    }
    assert order(markets)[0] == "wide"
    assert order(markets)[-1] == "gpu"


def test_series_breadth_is_the_max_over_its_skus():
    markets = {"big": market("a", regions=2), "small": market("a", regions=9), "b": market("b", regions=5)}
    assert order(markets) == ["small", "big", "b"]  # series a counts 9 regions for both of its SKUs, smaller first


def test_vendor_with_few_regions_orders_by_price():
    markets = {"dear": market("a", regions=2, price=3), "cheap": market("b", regions=1, price=0.5)}
    assert order(markets) == ["cheap", "dear"]


def test_unknown_series_orders_by_price_after_the_breadth_ranked():
    markets = {"cheap": market(None, price=0.1), "wide": market("w", regions=9), "dear": market(None, price=2)}
    markets["w2"] = market("x", regions=30)  # many priced regions: breadth applies to known series
    assert order(markets) == ["w2", "wide", "cheap", "dear"]


def test_tie_breaks_memory_generation_started_series_size_then_crc():
    markets = {
        "old": market("a", generation="DDR4"),
        "new": market("b", generation="DDR5"),
        "started": market("c", generation="DDR4"),
    }
    assert order(markets)[0] == "new"
    assert order(markets, started={"started"}) == ["new", "started", "old"]
    same = {"big": market("a", vcpus=8), "small": market("a", vcpus=2)}
    assert order(same) == ["small", "big"]
    twins = {"x": market("a"), "y": market("a")}
    expected = sorted(twins, key=lambda sku: zlib.crc32(f"v/{sku}".encode()))
    assert order(twins) == expected


def test_a_started_series_goes_before_an_equal_one():
    markets = {"fresh": market("a"), "begun": market("b")}
    assert order(markets, started={"begun"}) == ["begun", "fresh"]


def test_memory_generation_is_read_from_the_string():
    assert [popularity.memory_generation(t) for t in ("DDR5", "LPDDR4X", "", None, "HBM")] == [5, 4, 0, 0, 0]


def test_skus_without_data_follow_the_ranked_ones_by_crc():
    markets = {"known": market("a")}
    result = order(markets, skus=["z", "known", "y"])
    assert result[0] == "known"
    assert result[1:] == sorted(["z", "y"], key=lambda sku: zlib.crc32(f"v/{sku}".encode()))


@pytest.fixture
def calls(monkeypatch):
    seen = []
    monkeypatch.setattr(keeper, "region_ids", lambda env, vendor: seen.append("regions") or {"r0": "1"})
    monkeypatch.setattr(keeper, "sku_market", lambda env, vendor, sku, ids: seen.append(sku) or market(sku))
    return seen


def test_warm_asks_a_few_skus_per_round_and_keeps_the_answers(calls):
    cache = popularity.PopularityCache()
    skus = [f"s{i}" for i in range(5)]
    cache.warm(ENV, "v", skus, limit=2)
    assert calls == ["regions", "s0", "s1"]  # region ids once per vendor
    cache.warm(ENV, "v", skus, limit=2)
    assert calls == ["regions", "s0", "s1", "s2", "s3"]
    assert set(cache.markets("v")) == {"s0", "s1", "s2", "s3"}


def test_warm_refreshes_after_a_day(calls, monkeypatch):
    cache = popularity.PopularityCache()
    cache.warm(ENV, "v", ["a"])
    clock = [popularity.time.monotonic() + popularity.CACHE_SECONDS + 1]
    monkeypatch.setattr(popularity.time, "monotonic", lambda: clock[0])
    cache.warm(ENV, "v", ["a"])
    assert calls == ["regions", "a", "regions", "a"]


def test_a_keeper_failure_ends_the_round_and_keeps_what_is_cached(monkeypatch, caplog):
    monkeypatch.setattr(keeper, "region_ids", lambda env, vendor: {})
    answers = iter([market("a"), RuntimeError("keeper HTTP 503")])

    def sku_market(env, vendor, sku, ids):
        answer = next(answers)
        if isinstance(answer, Exception):
            raise answer
        return answer

    monkeypatch.setattr(keeper, "sku_market", sku_market)
    cache = popularity.PopularityCache()
    cache.warm(ENV, "v", ["a", "b", "c"])
    assert set(cache.markets("v")) == {"a"}  # "c" was not even tried
    assert "503" in caplog.text


def test_an_unknown_sku_is_remembered_as_unknown(monkeypatch):
    monkeypatch.setattr(keeper, "region_ids", lambda env, vendor: {})
    monkeypatch.setattr(keeper, "sku_market", lambda env, vendor, sku, ids: None)
    cache = popularity.PopularityCache()
    cache.warm(ENV, "v", ["a"])
    assert cache.markets("v") == {"a": None}


def test_sku_market_reads_series_breadth_inputs_and_prices(monkeypatch):
    server = {
        "series": "c6",
        "accelerator_count": None,
        "memory_generation": "DDR5",
        "vcpus": 8,
        "memory_amount": 16384,
    }
    rows = [
        {"region_id": 1, "allocation": "ondemand", "price": 2.0, "status": "active"},
        {"region_id": 1, "allocation": "ondemand", "price": 1.5, "status": "active"},
        {"region_id": 1, "allocation": "spot", "price": 0.4, "status": "active"},
        {"region_id": 2, "allocation": "ondemand", "price": 9, "status": "inactive"},
        {"region_id": 9, "allocation": "ondemand", "price": 1, "status": "active"},  # not a region of the vendor
    ]
    monkeypatch.setattr(keeper, "_get", lambda url, key: server)
    monkeypatch.setattr(keeper, "_fetch", lambda url, key: rows)
    result = keeper.sku_market(ENV, "aws", "c6.2xl", {"eu-1": "1", "eu-2": "2"})
    assert result == {
        "series": "c6", "accelerators": 0, "memory_generation": "DDR5", "vcpus": 8, "memory_gib": 16.0,
        "ondemand": {"eu-1": 1.5}, "spot": {"eu-1": 0.4},
    }  # fmt: skip
    monkeypatch.setattr(keeper, "_get", lambda url, key: None)
    assert keeper.sku_market(ENV, "aws", "nope", {}) is None
