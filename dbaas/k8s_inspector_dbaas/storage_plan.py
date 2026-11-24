"""Storage sizing that the vendor modules call: the vendor-independent part of the legacy `db_storage` and
`disk_profiles`, so every vendor's database gets a comparable I/O budget per vCPU.

A vendor module picks its disk type, describes it with a `DiskProfile` (the numbers are the vendor's own
documentation, so they live in the vendor module) and calls `plan`. Snapping to what the vendor accepts (size
steps, bands, flavour minimums) is the vendor module's job.
"""

import math
from dataclasses import dataclass

SCHEMA_RAM_FRAC = 0.25  # the schema should fit shared_buffers (a quarter of RAM)
SCHEMA_SIZE_GIB = (1.0, 4.0, 16.0, 64.0)
DISK_SCHEMA_RATIO = 2.0  # disk = schema x 2 ...
DISK_USABLE_FRAC = 0.85  # ... over the usable fraction of the volume
MIN_STORAGE_GIB = 64  # floor of the disk (OS parity, and GCP size-derived IOPS)
WRITE_MBPS_PER_VCPU = 1.5  # target I/O budget per vCPU
WRITE_IOPS_PER_VCPU = 25.0
MIN_WRITE_MBPS = 50  # never provision below these, even for 1 vCPU
MIN_WRITE_IOPS = 1000


@dataclass(frozen=True)
class DiskProfile:
    """Performance model of one disk type. Rates are per GiB of capacity up to the caps; for types whose IOPS and
    throughput are provisioned independently of the size (`independently_provisioned`) the per-GiB rates are 0."""

    min_size_gib: int
    max_write_iops: int
    max_write_mbps: int
    independently_provisioned: bool
    write_iops_per_gib: float = 0
    write_mbps_per_gib: float = 0
    write_iops_per_vcpu: float = 0  # per-VM cap that grows with the vCPU count; 0: no cap
    write_mbps_per_vcpu: float = 0


@dataclass(frozen=True)
class StoragePlan:
    storage_gib: int
    iops: int | None  # to provision; None: size-derived, the vendor handles it
    throughput_mb_s: int | None
    effective_write_iops: int | None = None  # informational
    effective_write_mbps: int | None = None


def schema_gib(mem_gib: float) -> float:
    """Largest schema rung that fits in a quarter of the RAM (a smaller fraction of a GiB when none does)."""
    budget = max(float(mem_gib), 0.0) * SCHEMA_RAM_FRAC
    fitting = [g for g in SCHEMA_SIZE_GIB if g <= budget + 1e-9]
    return fitting[-1] if fitting else max(0.0625, budget)


def storage_gib_for_mem(mem_gib: float) -> int:
    """Disk size from the RAM-scaled schema, floored at `MIN_STORAGE_GIB`."""
    need = schema_gib(mem_gib) * DISK_SCHEMA_RATIO
    return max(MIN_STORAGE_GIB, math.ceil(need / DISK_USABLE_FRAC))


def plan(mem_gib: float, vcpus: int, profile: DiskProfile | None = None) -> StoragePlan:
    """The disk to provision for a database of `mem_gib` and `vcpus`. Without a `profile` (the vendor sizes the
    disk only, no I/O knobs) the plan is the schema-derived size. With one, the I/O targets scale with `vcpus`:
    independently provisioned types get explicit IOPS and throughput, size-derived types a larger disk."""
    size = storage_gib_for_mem(mem_gib)
    if profile is None:
        return StoragePlan(storage_gib=size, iops=None, throughput_mb_s=None)
    vcpus = max(1, int(vcpus))
    mbps = max(MIN_WRITE_MBPS, vcpus * WRITE_MBPS_PER_VCPU)
    iops = max(MIN_WRITE_IOPS, vcpus * WRITE_IOPS_PER_VCPU)
    if profile.write_mbps_per_vcpu > 0:
        mbps = min(mbps, vcpus * profile.write_mbps_per_vcpu)
    if profile.write_iops_per_vcpu > 0:
        iops = min(iops, vcpus * profile.write_iops_per_vcpu)
    mbps, iops = min(mbps, profile.max_write_mbps), min(iops, profile.max_write_iops)
    if profile.independently_provisioned:
        provisioned_iops, provisioned_mbps = math.ceil(iops), math.ceil(mbps)
        return StoragePlan(
            storage_gib=max(profile.min_size_gib, size),
            iops=provisioned_iops,
            throughput_mb_s=provisioned_mbps,
            effective_write_iops=provisioned_iops,
            effective_write_mbps=provisioned_mbps,
        )
    size = max(profile.min_size_gib, size, _gib_for(profile.write_mbps_per_gib, mbps, profile.max_write_mbps))
    size = max(size, _gib_for(profile.write_iops_per_gib, iops, profile.max_write_iops))
    return StoragePlan(
        storage_gib=size,
        iops=None,
        throughput_mb_s=None,
        effective_write_iops=int(min(size * profile.write_iops_per_gib, profile.max_write_iops)),
        effective_write_mbps=int(min(size * profile.write_mbps_per_gib, profile.max_write_mbps)),
    )


def _gib_for(rate_per_gib: float, target: float, cap: float) -> int:
    """Smallest size that reaches `target` (or the cap) at `rate_per_gib`; 0 when the rate does not scale."""
    if rate_per_gib <= 0:
        return 0
    return math.ceil(min(target, cap) / rate_per_gib)
