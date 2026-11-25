"""The client server of a DBaaS run: the machine that drives pgbench against the managed database.

The managed database does the heavy work, so the client is small and its speed does not matter: any x86_64 server (the
benchmark image is amd64 only) without an accelerator, with 2-4 vCPUs and 4-8 GiB, dedicated or shared CPUs. The anchor
requires these as SCarpenter labels (`requirements`) and Karpenter launches the cheapest offered type that fits in the
client region, so a type that is blocked there (no quota of its family, say) is passed over. Keeper's servers
(`most_expensive`) only bound the price of a run.
"""

SCARPENTER = "scarpenter.sparecores.com/"
MIN_VCPUS, MAX_VCPUS = 2, 4
MIN_MEMORY_MIB, MAX_MEMORY_MIB = 4 * 1024, 8 * 1024
CPU_ALLOCATIONS = ["Dedicated", "Shared"]


def requirements(client: str | None = None) -> list[dict]:
    """The node affinity expressions of the client's anchor (on top of vendor, region, capacity type and run): the
    band above, with `accelerator-count` undefined on types without an accelerator. An explicit `client` SKU replaces
    the band."""
    if client:
        return [{"key": f"{SCARPENTER}api-reference", "operator": "In", "values": [client]}]
    return [
        {"key": "kubernetes.io/arch", "operator": "In", "values": ["amd64"]},
        {"key": f"{SCARPENTER}vcpus", "operator": "Gt", "values": [str(MIN_VCPUS - 1)]},
        {"key": f"{SCARPENTER}vcpus", "operator": "Lt", "values": [str(MAX_VCPUS + 1)]},
        {"key": f"{SCARPENTER}memory-mib", "operator": "Gt", "values": [str(MIN_MEMORY_MIB - 1)]},
        {"key": f"{SCARPENTER}memory-mib", "operator": "Lt", "values": [str(MAX_MEMORY_MIB + 1)]},
        {"key": f"{SCARPENTER}accelerator-count", "operator": "DoesNotExist"},
        {"key": f"{SCARPENTER}cpu-allocation", "operator": "In", "values": CPU_ALLOCATIONS},
    ]


def _usable(server: dict) -> bool:
    """A Keeper server that `requirements` would accept."""
    return (
        server.get("cpu_architecture") == "x86_64"
        and not server.get("accelerator_count")
        and MIN_VCPUS <= (server.get("vcpus") or 0) <= MAX_VCPUS
        and MIN_MEMORY_MIB <= (server.get("memory_amount") or 0) <= MAX_MEMORY_MIB
        and server.get("cpu_allocation") in CPU_ALLOCATIONS
        and server.get("min_price_ondemand") is not None
    )


def most_expensive(servers: list[dict]) -> float | None:
    """The highest on-demand price among the servers that `requirements` accepts (the most the cluster may launch),
    None when there is none."""
    return max((float(s["min_price_ondemand"]) for s in servers if _usable(s)), default=None)
