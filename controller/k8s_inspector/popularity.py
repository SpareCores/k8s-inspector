"""Popularity order of the autoPlan candidates, from Keeper data (docs/reference/configuration.md "Order and packing").

`PopularityCache` keeps what Keeper says about each SKU (`keeper.sku_market`) for a day and fills itself a few SKUs
per planning cycle, so the public Keeper only ever sees small, sequential calls. `rank` turns that data into a sort
key per SKU; a SKU without data sorts after the ranked ones in `start_order` (crc32), so planning never waits for
Keeper.
"""

import logging
import re
import time
import zlib
from dataclasses import dataclass

from k8s_inspector import keeper
from k8s_inspector.model import EnvConfig

log = logging.getLogger(__name__)

CACHE_SECONDS = 24 * 3600
FAILURE_SECONDS = 6 * 3600  # a SKU Keeper failed for is not asked again this long
COOLDOWN_SECONDS = 10 * 60  # after a Keeper failure no vendor asks for this long
LOOKUPS_PER_CYCLE = 40  # SKUs asked per planning cycle over all vendors, two Keeper calls each
PRICE_ORDER_MAX_REGIONS = 3  # a vendor with this few priced regions has no region breadth to rank by


class PopularityCache:
    """`keeper.sku_market` of each SKU, and the region ids of each vendor, kept for `CACHE_SECONDS`. Stale data is
    used until the SKU is asked again. A cycle (`begin_cycle`) asks about `lookups_per_cycle` SKUs at most, over all
    vendors. A failure starts a cooldown for every vendor, and the SKU it happened on is not asked again for
    `FAILURE_SECONDS`."""

    def __init__(self, lookups_per_cycle: int = LOOKUPS_PER_CYCLE) -> None:
        self._skus: dict[tuple[str, str], tuple[float, dict | None]] = {}  # (vendor, sku) -> (asked, market)
        self._failed: dict[tuple[str, str], float] = {}  # (vendor, sku) -> when Keeper failed for it
        self._region_ids: dict[str, tuple[float, dict[str, str]]] = {}
        self._cooldown_since = -COOLDOWN_SECONDS
        self._lookups_per_cycle = lookups_per_cycle
        self._left = lookups_per_cycle

    def begin_cycle(self) -> None:
        self._left = self._lookups_per_cycle

    def markets(self, vendor: str) -> dict[str, dict | None]:
        """`{sku: market}` of the vendor's SKUs asked so far; None for a SKU Keeper does not know."""
        return {sku: market for (v, sku), (_, market) in self._skus.items() if v == vendor}

    def warm(self, env: EnvConfig, vendor: str, skus: list[str]) -> None:
        """Ask Keeper about the first of `skus` that have no fresh data, one after the other, while the cycle's budget
        lasts. The first failure ends the round for every vendor until the cooldown is over."""
        now = time.monotonic()
        if now - self._cooldown_since < COOLDOWN_SECONDS:
            return
        due = [
            sku
            for sku in skus
            if now - self._skus.get((vendor, sku), (-CACHE_SECONDS, None))[0] >= CACHE_SECONDS
            and now - self._failed.get((vendor, sku), -FAILURE_SECONDS) >= FAILURE_SECONDS
        ]
        for sku in due[: self._left]:
            self._left -= 1
            try:
                ids = self._ids(env, vendor, now)
            except Exception as e:  # not the SKU's fault: cool down, no negative entry
                log.warning("popularity: Keeper region lookup for %s failed, no lookups for a while: %s", vendor, e)
                self._cooldown_since = now
                return
            try:
                self._skus[vendor, sku] = (now, keeper.sku_market(env, vendor, sku, ids))
            except Exception as e:  # RuntimeError, or an unreadable key file: rank with what is cached
                log.warning("popularity: Keeper lookup for %s/%s failed, no lookups for a while: %s", vendor, sku, e)
                self._failed[vendor, sku] = self._cooldown_since = now
                return

    def _ids(self, env: EnvConfig, vendor: str, now: float) -> dict[str, str]:
        asked, ids = self._region_ids.get(vendor, (-CACHE_SECONDS, {}))
        if now - asked >= CACHE_SECONDS:
            ids = keeper.region_ids(env, vendor)
            self._region_ids[vendor] = (now, ids)
        return ids


@dataclass(frozen=True)
class Score:
    key: tuple  # sorts the candidates: smaller first
    note: str  # the components, for the log


def memory_generation(text: str | None) -> int:
    """The number in a memory type like "DDR5" or "LPDDR4X" (newer is larger), 0 when there is none."""
    found = re.search(r"\d+", text or "")
    return int(found.group()) if found else 0


def rank(vendor: str, skus: list[str], markets: dict[str, dict | None], started: set[str]) -> dict[str, Score]:
    """A `Score` for each of `skus`. `markets` is `PopularityCache.markets(vendor)`; `started` are the SKUs that
    already have a run or a result from k8s-inspector. Smaller keys first:

    1. SKUs with accelerators last;
    2. a SKU of a series with the widest region breadth first, the breadth being the most regions with an on-demand
       price of any SKU of the series. Where breadth means nothing (the vendor's priced regions are at most
       `PRICE_ORDER_MAX_REGIONS`, or Keeper has no series for the SKU) the cheapest on-demand price goes first;
    3. newer memory generation, a series that is started already, the series (so its SKUs stay together),
       smaller SKU first (vCPUs, then memory), and crc32 of `vendor/sku` for the rest.

    A SKU without Keeper data (or without any on-demand price) sorts after the ranked ones of its kind, by crc32."""
    breadth: dict[str, int] = {}
    priced_regions: set[str] = set()
    for market in markets.values():
        if market and market["series"]:
            breadth[market["series"]] = max(breadth.get(market["series"], 0), len(market["ondemand"]))
        if market:
            priced_regions.update(market["ondemand"])
    by_price = len(priced_regions) <= PRICE_ORDER_MAX_REGIONS
    started_series = {m["series"] for sku in started if (m := markets.get(sku)) and m["series"]}

    scores = {}
    for sku in skus:
        crc = zlib.crc32(f"{vendor}/{sku}".encode())
        market = markets.get(sku)
        if not market or not market["ondemand"]:
            scores[sku] = Score((0, 2, 0, 0, 1, 0, 0, 0, crc, sku), "no Keeper data")
            continue
        series = market["series"]
        if by_price or not series:
            tier, primary = 1, min(market["ondemand"].values())
            note = f"cheapest on-demand {primary:g} USD/h"
        else:
            tier, primary = 0, -breadth[series]
            note = f"series {series} in {breadth[series]} regions"
        generation = memory_generation(market["memory_generation"])
        key = (
            1 if market["accelerators"] else 0,
            tier,
            primary,
            -generation,
            0 if series in started_series else 1,
            zlib.crc32(f"{vendor}/{series}".encode()) if series else 0,
            market["vcpus"] or 0,
            market["memory_gib"] or 0,
            crc,
            sku,
        )
        kind = "accelerator" if market["accelerators"] else "cpu"
        scores[sku] = Score(key, f"{note}, memory gen {generation}, {kind}")
    return scores
