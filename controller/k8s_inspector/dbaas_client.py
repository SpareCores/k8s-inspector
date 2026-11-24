"""The client server of a DBaaS run: port of sc-inspector `rank_dbaas_client_instances`, over Keeper's server data.

The managed database does the heavy work, so the client driving pgbench is small and fixed whatever the database is:
x86_64 (the benchmark image is amd64 only), no accelerator, 2-4 vCPUs, 4-8 GiB. The steady CPUs of a dedicated
instance are preferred; a region without any in that band (UpCloud lists every VM as shared) gets shared ones. The
cheapest wins, after a Tukey price fence drops genuine outliers (a small but exotic SKU) of the region's own pool.
"""

import statistics

MIN_VCPUS, MAX_VCPUS = 2, 4
MIN_MEMORY_MIB, MAX_MEMORY_MIB = 4 * 1024, 8 * 1024
MIN_FOR_FENCE = 4  # fewer prices make quartiles too noisy to trust


def _usable(server: dict) -> bool:
    return (
        server.get("cpu_architecture") == "x86_64"
        and not server.get("accelerator_count")
        and MIN_VCPUS <= (server.get("vcpus") or 0) <= MAX_VCPUS
        and MIN_MEMORY_MIB <= (server.get("memory_amount") or 0) <= MAX_MEMORY_MIB
        and server.get("min_price_ondemand") is not None
    )


def _fence(prices: list[float]) -> float | None:
    """Q3 + 1.5 x IQR, None when there are too few prices."""
    if len(prices) < MIN_FOR_FENCE:
        return None
    q1, _, q3 = statistics.quantiles(prices, n=4)
    return q3 + 1.5 * (q3 - q1)


def rank(servers: list[dict]) -> list[str]:
    """The `api_reference` of the usable servers, best first: dedicated before shared, then cheapest, then by name."""
    usable = [s for s in servers if _usable(s)]
    pool = [s for s in usable if s.get("cpu_allocation") == "Dedicated"] or [
        s for s in usable if s.get("cpu_allocation") in ("Dedicated", "Shared")
    ]
    fence = _fence([float(s["min_price_ondemand"]) for s in pool])
    if fence is not None:
        pool = [s for s in pool if float(s["min_price_ondemand"]) <= fence]
    pool.sort(
        key=lambda s: (s.get("cpu_allocation") != "Dedicated", float(s["min_price_ondemand"]), s["api_reference"])
    )
    return [s["api_reference"] for s in pool]
