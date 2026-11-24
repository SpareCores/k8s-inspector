import pytest

from k8s_inspector_dbaas import storage_plan
from k8s_inspector_dbaas.storage_plan import DiskProfile

# The documented numbers of the legacy disk_profiles registry (GCP pd-ssd, AWS gp3): the plan must give the same
# sizes and I/O budgets as sc-inspector did, so results stay comparable.
PD_SSD = DiskProfile(
    min_size_gib=10, max_write_iops=30_000, max_write_mbps=400, independently_provisioned=False,
    write_iops_per_gib=30, write_mbps_per_gib=0.48, write_iops_per_vcpu=800, write_mbps_per_vcpu=6,
)  # fmt: skip
GP3 = DiskProfile(min_size_gib=1, max_write_iops=16_000, max_write_mbps=1000, independently_provisioned=True)


@pytest.mark.parametrize(
    ("mem_gib", "schema"), [(2, 0.5), (4, 1.0), (15.9, 1.0), (16, 4.0), (64, 16.0), (256, 64.0), (1024, 64.0)]
)
def test_schema_is_the_largest_rung_in_a_quarter_of_the_ram(mem_gib, schema):
    assert storage_plan.schema_gib(mem_gib) == schema


def test_disk_size_has_a_floor_and_follows_the_schema():
    assert storage_plan.storage_gib_for_mem(4) == 64
    assert storage_plan.storage_gib_for_mem(256) == 151  # ceil(64 GiB schema * 2 / 0.85)


def test_without_a_profile_only_the_size_is_planned():
    plan = storage_plan.plan(32, 8)
    assert (plan.storage_gib, plan.iops, plan.throughput_mb_s) == (64, None, None)


def test_independent_io_gets_explicit_iops_and_throughput_per_vcpu():
    plan = storage_plan.plan(64, 8, GP3)
    assert (plan.storage_gib, plan.iops, plan.throughput_mb_s) == (
        64,
        1000,
        50,
    )  # the floors: 8 x 25 IOPS, 8 x 1.5 MB/s
    big = storage_plan.plan(512, 128, GP3)
    assert (big.iops, big.throughput_mb_s) == (3200, 192)
    capped = storage_plan.plan(2048, 4096, GP3)
    assert (capped.iops, capped.throughput_mb_s) == (16_000, 1000)


def test_size_derived_io_grows_the_disk():
    plan = storage_plan.plan(512, 128, PD_SSD)  # 192 MB/s at 0.48 MB/s per GiB: 400 GiB; 3200 IOPS: 107 GiB
    assert plan.storage_gib == 400 and plan.iops is None
    assert plan.effective_write_mbps == 192 and plan.effective_write_iops == 12_000
    small = storage_plan.plan(16, 2, PD_SSD)  # the per-VM cap (2 x 6 MB/s) is below what 64 GiB gives
    assert small.storage_gib == 64
