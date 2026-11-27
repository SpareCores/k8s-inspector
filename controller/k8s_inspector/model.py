"""Shared dataclasses and enums. Data only: no logic, no I/O, no imports from other k8s_inspector modules.

Every module codes against these types. Conversion to and from Kubernetes dicts or JSON lives in the module that
owns the format (`crd.py` for BenchmarkRun, `pods.py` for Pods, `storage.py` for urls.json and manifests).

Conventions:
- Times are timezone-aware UTC `datetime`s (the publisher writes naive UTC to meta.json, see output-contract.md).
- "key" of a container/task output prefix is the name used in `urls.json` and in object storage under
  `tasks/<key>/`. It equals the task name, except for vLLM probes (`vllm-probe-<image>`).
- `object key` always means the full S3 key; `urls key` means `<key>/<file>` as in urls.json.
"""

import math
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from enum import IntEnum, StrEnum

# --------------------------------------------------------------------------- enums


class CapacityMode(StrEnum):
    """`spec.capacity` and the per-vendor setting."""

    SPOT_FIRST = "SpotFirst"  # spot preferred, Karpenter falls back to on-demand by itself
    ON_DEMAND = "OnDemand"
    SPOT_UNTIL_EXHAUSTED = (
        "SpotUntilExhausted"  # spot only until the SKU is exhausted (`spot.is_exhausted`), then SpotFirst
    )
    SPOT_ONLY = "SpotOnly"  # never on-demand


class CapacityType(StrEnum):
    """`karpenter.sh/capacity-type` values; `status.capacityType`."""

    SPOT = "spot"
    ON_DEMAND = "on-demand"


class RunPhase(StrEnum):
    """`status.phase` (objects.md)."""

    PENDING = "Pending"
    PROVISIONING = "Provisioning"
    RUNNING = "Running"
    RELEASING = "Releasing"
    PUBLISHING = "Publishing"
    DONE = "Done"
    FAILED = "Failed"
    CANCELLED = "Cancelled"


# In the error of an up Job that cannot create the database in its region, whatever else happens (the provisioner's
# `RegionUnavailable`): the run ends with this reason and the next plan tries other regions first.
VENDOR_WIDE = "[vendor-wide]"  # after it: no database of the vendor can be created in the region
REGION_UNAVAILABLE = "DBAAS_REGION_UNAVAILABLE:"


class DbPhase(StrEnum):
    """`status.database.phase`: where the managed database of a DBaaS run is. Absent: nothing was created yet."""

    CLEARING = "Clearing"  # a stack of an earlier run of this name is destroyed first; then the run starts over
    CREATING = "Creating"  # the Job `dbaas-up-<run>` is (about to be) running
    BOOTSTRAPPING = "Bootstrapping"  # the database exists; the bootstrap Pod prepares it on the client node
    READY = "Ready"  # the benchmark may run
    DESTROYING = "Destroying"  # the Job `dbaas-down-<run>` runs until it succeeds
    DESTROYED = "Destroyed"  # the stack is destroyed and its state removed; the finalizer is gone


class TaskState(StrEnum):
    """`status.tasks[].state`. A failed task is `DONE` with a non-zero `exit_code`."""

    PENDING = "Pending"
    RUNNING = "Running"
    DONE = "Done"


class AttemptOutcome(StrEnum):
    """`status.attempts[].outcome`."""

    COMPLETED = "Completed"
    SPOT_INTERRUPTED = "SpotInterrupted"
    NODE_LOST = "NodeLost"
    PROVISIONING_FAILED = "ProvisioningFailed"
    LAUNCH_FAILED = "LaunchFailed"  # a NodeClaim of a spot-allowed attempt failed to launch or boot: on-demand next
    # A spot-only attempt found no spot capacity: no tasks ran, the SKU is tried later.
    SPOT_UNAVAILABLE = "SpotUnavailable"
    # A SpotOnly attempt's spot launch was rejected for the SKU: no tasks ran, spot is not planned for it for a while.
    SPOT_REJECTED = "SpotRejected"
    CANCELLED = "Cancelled"
    HARNESS_ERROR = "HarnessError"  # the controller gave up on the run


class PodRole(StrEnum):
    """Label `inspector.sparecores.com/role`."""

    ANCHOR = "anchor"
    STEP = "step"
    FLUSH = "flush"
    DBAAS = "dbaas"


class ExitCode(IntEnum):
    """Special `meta.json` exit codes (exit-codes.md). A task's own positive exit code is a plain int."""

    OK = 0
    HARNESS_ERROR = 256
    NO_NODE = -1
    PRECHECK_FAILED = -3
    NEVER_FINISHED = -4
    RETRIES_EXHAUSTED = -5
    TIMEOUT = -6


class Reason(StrEnum):
    """`error_msg` values for exit code -4 (and the OOM marker for exit code 137)."""

    SPOT_INTERRUPTED = "SPOT_INTERRUPTED"
    NODE_LOST = "NODE_LOST"
    IMAGE_PULL_TIMEOUT = "IMAGE_PULL_TIMEOUT"
    EVICTED = "EVICTED"
    DEADLINE_EXCEEDED = "DEADLINE_EXCEEDED"
    CANCELLED = "CANCELLED"
    OUTPUTS_LOST = "OUTPUTS_LOST"
    OOM_KILLED = "OOMKilled"


# --------------------------------------------------------------------------- task catalogue


@dataclass(frozen=True)
class TaskDef:
    """One benchmark task of the catalogue (tasks.md "Fields"). Mirrors sc-inspector's `Task`/`DockerTask`.

    Hash inputs (see `tasks.task_hash`): `command`, `transforms` (their legacy source text), `image`,
    `workload_proxy`, `durability`, and `revision` when > 0. Nothing else may change a hash.

    `command` and `version_command` are legacy values: arguments appended to `entrypoint` as in
    `docker run <image> <command>` (`pods.py` builds the argv and the `sh -c` text for sc-run from them).
    """

    name: str
    image: str  # legacy reference with tag, HASHED. vLLM: the three images joined with "|"
    command: str | None  # legacy command string, HASHED. None is hashed as "None"
    timeout: timedelta
    digest: str | None = None  # "sha256:...", pins the Pod image, NOT hashed
    images: tuple[str, ...] = ()  # vLLM image ladder in order; empty for every other task
    image_digests: dict[str, str] = field(default_factory=dict)  # ladder: image reference -> digest, NOT hashed
    entrypoint: tuple[str, ...] = ()  # the image's ENTRYPOINT restated (Pod `command` replaces it)
    version_command: str | None = None  # run by sc-run before the task, 60 s cap
    priority: float = float("inf")  # lower runs earlier; inf = last (storage, stressnglongrun)
    parallel: bool = False  # True: part of the probe step (one Pod, one container per task)
    transforms: tuple[str, ...] = ("raw",)  # "raw", "fetch_geekbench_results"
    parse: str | None = None  # publisher parser: dmidecode | openssl | stressnglongrun | lstopo
    privileges: frozenset[str] = frozenset()  # "host_pid" (storage), "db_rlimits" (pgbench)
    host_network: bool = True  # False: the Pod's own network namespace (loopback-only tasks)
    memory_cap: float | None = None  # fraction of Node allocatable memory as limit (0.85 geekbench, vllm)
    shm_gib: int | None = None  # /dev/shm size (4 for vllm)
    gpu: bool = False  # needs the GPU, no CPU fallback
    precheck_command: str | None = None
    precheck_regex: str | None = None  # precheck: condition against node labels or probe output -> -3
    secrets: tuple[str, ...] = ()  # keys of Secret `inspector-secrets` this task gets
    env: tuple[tuple[str, str], ...] = ()  # fixed variables (TRACKER_* are added by pods.py)
    collect: tuple[str, ...] = ()  # sc-run --collect specs "src:name"
    large_outputs: tuple[str, ...] = ()  # file names that get multipart slots from the start
    workload_proxy: str | None = None  # HASHED when set
    dbaas: bool = False  # DBaaS task: runs only in a DBaaS run, never in the default server selection
    durability: str | None = None  # HASHED when set
    revision: int = 0  # deliberate rerun; hashed only when > 0
    keeper_gated: bool = False  # Keeper-allowlisted (stressnglongrun): only when named, never in a default selection
    spot: bool = True  # False: runs containing this task are on-demand (stressnglongrun)
    models_cache: bool = False  # mount the per-node model hostPath at /models (llm, vllm)


@dataclass(frozen=True)
class StepContainer:
    """One container of a step Pod: a task, possibly a specific vLLM probe image or an adapted command."""

    key: str  # output prefix and urls.json prefix: task name or `vllm-probe-<image>`
    task: TaskDef
    image: str  # full Pod image reference including digest
    command: str | None = None  # overrides task.command (vLLM probe, nvbandwidth -b ...)
    timeout: timedelta | None = None  # overrides task.timeout (vLLM probe: 18 min)
    env: tuple[tuple[str, str], ...] = ()  # extra variables (GPU_VRAM_MIB, ...)


@dataclass(frozen=True)
class Step:
    """One Pod of a run attempt: one serial task, or the parallel probe group."""

    index: int  # position within the attempt, starts at 1
    name: str  # Pod name suffix, e.g. "07-geekbench" or "01-probes"
    containers: tuple[StepContainer, ...]
    gpu_runtime: bool = False  # runtimeClassName: nvidia (GPU types; dropped by the GPU->CPU retry)


# --------------------------------------------------------------------------- run identity, owners


@dataclass(frozen=True)
class RunKey:
    """Where a run's objects live in object storage. `day` is the run's creation date (UTC), fixed."""

    vendor: str
    sku: str
    day: date
    run_id: str


@dataclass(frozen=True)
class OwnerRef:
    """Owner reference target (the BenchmarkRun for the anchor, the anchor Pod for steps)."""

    api_version: str
    kind: str
    name: str
    uid: str


# --------------------------------------------------------------------------- BenchmarkRun (objects.md)


@dataclass
class RunSpec:
    vendor: str
    sku: str  # a DBaaS run has no server of its own: its `sku` is the database id
    run_id: str
    tasks: list[str] = field(default_factory=list)  # empty = every due task
    force: bool = False
    capacity: CapacityMode = CapacityMode.SPOT_FIRST
    hold_after: timedelta = timedelta(0)
    database: dict | None = None  # DBaaS: {vendor, id, engineVersion, region, client}
    regions: list[str] = field(default_factory=list)  # a server run the planner confined to these regions; empty: any


@dataclass
class TaskStatus:
    """`status.tasks[]`. Also used for vLLM probe keys (name = key)."""

    name: str
    state: TaskState = TaskState.PENDING
    attempt: int | None = None
    start: datetime | None = None
    end: datetime | None = None
    exit_code: int | None = None
    error_msg: str | None = None
    published: bool = False
    commit: str | None = None
    gpu_fallback: bool = False  # step was recreated once without the GPU runtime class


@dataclass
class AttemptStatus:
    """`status.attempts[]`."""

    n: int
    capacity_type: CapacityType | None = None  # of the node the attempt got; None while it has none
    node_claim: str | None = None
    node: str | None = None
    start: datetime | None = None
    end: datetime | None = None
    outcome: AttemptOutcome | None = None


@dataclass
class LaunchFailure:
    """`status.launchFailures[]`: a NodeClaim of the current attempt that failed to launch (`LaunchFailed`)."""

    node_claim: str
    message: str
    time: datetime


@dataclass
class UninitializedClaim:
    """`status.uninitializedClaims[]`: a NodeClaim of the current attempt seen `Launched` and not yet `Initialized`.
    Karpenter deletes a claim that never registers, so the run remembers it to notice that it is gone."""

    node_claim: str
    launched: datetime
    capacity_type: str | None = (
        None  # `karpenter.sh/capacity-type` of the claim; None (older entry) counts as on-demand
    )


@dataclass
class DbStatus:
    """`status.database` of a DBaaS run. The controller writes the phase before it creates what the phase stands for,
    so a restarted controller knows there may be a database to destroy."""

    phase: DbPhase
    since: datetime  # the phase began
    expires: datetime  # the database is a leak after this (tagged on it, alerted on)
    error: str | None = None  # why the database was not created or not usable
    destroy_failures: int = 0  # failed `dbaas-down-<run>` Jobs so far
    retry_after: datetime | None = None  # the next destroy Job is created at this time
    # Timing file names (`api_start`, `api_end`, `api_destroy_start`, `api_destroy_end`) -> when: the create Job was
    # about to start / succeeded (the bootstrap starts), the destroy Job was about to start / succeeded.
    timing: dict[str, datetime] = field(default_factory=dict)


@dataclass
class RunStatus:
    phase: RunPhase = RunPhase.PENDING
    progress: str = "0/0"
    current_step: str | None = None  # name of the step Pod created last; the next step's number is its plus one
    step: dict | None = None  # the step in flight as `steps.to_status` wrote it; None between steps
    attempt: int = 0
    capacity_type: CapacityType | None = None
    node: str | None = None
    client_sku: str | None = None  # DBaaS: the SKU of the client node the cluster launched
    region: str | None = None
    zone: str | None = None
    price_per_hour: str | None = None
    price_incomplete: bool = (
        False  # SCarpenter's `price-incomplete` NodeClaim annotation: the price understates the cost
    )
    variant: int | None = None
    tasks: list[TaskStatus] = field(default_factory=list)
    attempts: list[AttemptStatus] = field(default_factory=list)
    launch_failures: list[LaunchFailure] = field(default_factory=list)  # of the current attempt, last 20
    uninitialized_claims: list[UninitializedClaim] = field(default_factory=list)  # of the current attempt
    spot_reclaims: int = 0  # spot claims of the current attempt reclaimed or gone before they were Initialized
    conditions: list[dict] = field(default_factory=list)
    database: DbStatus | None = None  # DBaaS runs


@dataclass
class BenchmarkRun:
    """Python view of a BenchmarkRun object. `name` is the lease (see `crd.run_name`)."""

    name: str
    namespace: str
    uid: str
    resource_version: str
    created: datetime
    spec: RunSpec
    status: RunStatus = field(default_factory=RunStatus)
    cancel_requested: bool = False  # annotation `inspector.sparecores.com/cancel: "true"` (crd.request_cancel)
    deleting: bool = (
        False  # `metadata.deletionTimestamp` is set: the DBaaS finalizer keeps the object until `down` succeeded
    )

    @property
    def node_sku(self) -> str | None:
        """The SKU of the run's node: a server run's own, a DBaaS run's client once it has a node (else None)."""
        return self.status.client_sku if self.spec.database else self.spec.sku


# --------------------------------------------------------------------------- node, outcomes


@dataclass
class NodeFacts:
    """What is known about the benchmark node: Node and NodeClaim from Kubernetes plus probe-step results."""

    node_name: str
    instance_type: str  # node.kubernetes.io/instance-type, "<vendor>.<sku>"
    arch: str  # amd64 | arm64
    created: datetime  # Node creationTimestamp (must be after the anchor was created)
    allocatable_memory_bytes: int
    allocatable_cpu_millicores: int = 0  # 0: unknown
    vcpus: int | None = None
    memory_bytes: int | None = None
    region: str | None = None
    zone: str | None = None
    capacity_type: CapacityType | None = None
    price_per_hour: str | None = None
    price_incomplete: bool = False
    node_claim: str | None = None
    nodeclass_hash: str | None = None
    node_os: str | None = None  # Node status.nodeInfo.osImage
    kubelet: str | None = None
    runtime: str | None = None  # container runtime version, "containerd://2.x" -> recorded as k8s-containerd
    has_nvidia_gpu: bool = False  # accelerator labels
    # Filled after the probe step:
    gpu_vram_mib: int | None = None
    gpu_compute_cap: tuple[int, int] | None = None  # minimum over GPUs, e.g. (8, 0)
    lscpu: dict[str, str] = field(default_factory=dict)  # parsed `lscpu -JB` fields, "Model name:" style keys
    boot_time: datetime | None = None  # kernel boot (sc-run result.json `btime`) -> machine_start timing


@dataclass
class TaskOutcome:
    """Result of one container/task, from sc-run's result.json and termination message plus Pod facts."""

    key: str
    exit_code: int  # ExitCode or the task's own positive code
    error_msg: str | None = None
    start: datetime | None = None
    end: datetime | None = None
    version: str | None = None
    kernel_version: str | None = None
    btime: datetime | None = None  # kernel boot time (sc-run result.json `btime`)
    timed_out: bool = False
    cancelled: bool = False
    oom_kills: int = 0
    nice: int | None = None  # nice the task ran at (sc-run result.json `nice`); None: disabled, not settable or unknown
    upload_complete: bool | None = None  # None: unknown (no termination message)
    missing_files: int = 0
    container_start_error: bool = False  # StartError / CreateContainerError / RunContainerError
    from_sc_run: bool = True  # False: derived from Pod state only (no termination message / result.json)


# --------------------------------------------------------------------------- uploads


@dataclass(frozen=True)
class UploadPlan:
    """Upload slot of one output file. One entry per `<key>/<file>` of urls.json.

    `upload_id`, `part_size`, `n_parts` are durable (a re-sign keeps them). The URL fields are empty until
    `Storage.presign` fills them; they are bearer secrets and must never be logged.
    """

    urls_key: str  # "<key>/<file>"
    object_key: str  # full S3 key
    upload_id: str | None = None  # set for multipart-capable files
    part_size: int = 0
    n_parts: int = 0
    put_url: str = ""
    part_urls: tuple[str, ...] = ()


@dataclass(frozen=True)
class ManifestPart:
    n: int
    size: int
    etag: str


@dataclass(frozen=True)
class ManifestObject:
    """One entry of sc-run's manifest.json `objects` (objects.md "manifest.json")."""

    name: str
    method: str  # "put" | "multipart"
    size: int
    md5: str | None = None
    etag: str | None = None
    upload_id: str | None = None
    part_size: int = 0
    parts: tuple[ManifestPart, ...] = ()


@dataclass(frozen=True)
class Manifest:
    task: str
    complete: bool
    objects: tuple[ManifestObject, ...]
    missing: tuple[str, ...] = ()


@dataclass(frozen=True)
class Verification:
    """Result of `Storage.finish_task`: multipart uploads completed and every object HEAD-checked."""

    key: str
    ok: bool
    manifest: Manifest | None
    problems: tuple[str, ...] = ()  # "manifest.json missing", "<file>: size 1 != manifest 2", ...


# --------------------------------------------------------------------------- configuration (configuration.md)


@dataclass(frozen=True)
class EnvConfig:
    """Deployment environment variables. Secrets are paths to mounted files, never values."""

    bucket: str
    bucket_region: str
    role_arn: str | None = None
    git_url: str = ""
    git_branch: str = "main"
    git_key_file: str | None = None
    keeper_api_base: str | None = None
    keeper_api_key_file: str | None = None
    namespace: str = "inspector"
    sc_run_image: str = ""  # K8SI_SC_RUN_IMAGE: "ghcr.io/sparecores/sc-run@sha256:..." (not in configuration.md)
    s3_endpoint_url: str | None = None  # K8SI_S3_ENDPOINT_URL: S3-compatible endpoint (OCI), optional
    key_prefix: str = ""  # K8SI_KEY_PREFIX: prepended to every object key (e.g. "k8s-inspector/spikes/"), optional
    dbaas_image: str = (
        ""  # K8SI_DBAAS_IMAGE: "ghcr.io/sparecores/k8s-inspector-dbaas@sha256:...", only DBaaS runs need it
    )
    dbaas_role_arn: str | None = (
        None  # K8SI_DBAAS_ROLE_ARN: the role of the Jobs for `dbaas-state/` (web identity); None: none
    )


UNLIMITED = math.inf  # `maxInFlight: unlimited`


@dataclass(frozen=True)
class VendorSettings:
    enabled: bool = False
    auto_plan: bool | None = None  # None: inherit planner.autoPlan
    max_in_flight: int | float | None = None  # None: defaults.maxInFlight; 0 pauses; UNLIMITED: no limit
    max_usd_per_hour: float | None = None
    max_new_runs_per_cycle: int = 10  # burst cap: runs one planning cycle creates for the vendor
    capacity: CapacityMode = CapacityMode.SPOT_FIRST  # same default as `RunSpec` and `crd.from_dict`
    publish: bool | tuple[str, ...] = False  # True: every SKU; a tuple: only these SKUs; False: none
    dbaas_auto_plan: bool = False  # autoPlan also creates DBaaS runs for this vendor
    dbaas_max_in_flight: int = (
        1  # DBaaS runs holding a database or client node at once; 0 pauses; server runs not counted
    )
    dbaas_max_usd_per_hour: float | None = None  # database + client node price of the DBaaS runs; None: no cap


@dataclass(frozen=True)
class Defaults:
    max_in_flight: int | float = 2  # UNLIMITED: no limit


@dataclass(frozen=True)
class SpotSettings:
    max_spot_attempts: int = 2
    max_lost_minutes: int = 60
    interruptions_to_quarantine: int = 2
    quarantine_days: int = 14
    spot_max_task_minutes: int = 240
    quota_reserve_after_minutes: int = 60  # a SKU deferred this long as spot-quota-busy reserves its buckets; 0: never
    patience_days: int = 14  # SpotUntilExhausted: a SKU is exhausted when its first counted try is this old ...
    coverage: int = 100  # ... and counted tries fell in this percent of the week's slots
    slot_hours: int = 3  # a slot is this many UTC hours of a weekday


@dataclass(frozen=True)
class Deadlines:
    provision_cpu_minutes: int = 60  # until a NodeClaim launches (a launch that keeps failing is ICE)
    provision_gpu_hours: int = 6  # ... for a GPU SKU
    registration_minutes: int = 8  # a launched node that has not registered this long ends the run BOOT_FAILURE
    registration_minutes_bare_metal: int = 14  # Karpenter's own limit is 15 min: keep these below it
    ready_minutes: int = 15  # a registered node that is not Ready this long after registering ends it BOOT_FAILURE
    ready_minutes_bare_metal: int = 30
    ready_minutes_gpu: int = 45  # driver sysext download and install
    anchor_ready_minutes: int = 15  # the node is Initialized and the anchor is not Ready this long: ANCHOR_FAILED
    no_offering_minutes: int = 10  # an unschedulable anchor without a NodeClaim this long ends -1 NO_OFFERING
    no_offering_fast_minutes: int = 2  # ... or this long when Karpenter's event says the bench pool cannot fit it
    image_pull_minutes: int = 15
    hold_idle_minutes: int = 45


@dataclass(frozen=True)
class PublishSettings:
    interval_minutes: int = 5
    keep_run_hours: int = 24


@dataclass(frozen=True)
class UploadSettings:
    part_mib: int = 16
    max_parts: int = 64
    single_put_max_mib: int = 32
    reput_seconds: int = 60
    url_ttl_hours: int = 12
    resign_every_minutes: int = 60


@dataclass(frozen=True)
class DbaasVendor:
    """`dbaas.vendors.<vendor>`: infrastructure the cluster platform provides for that vendor's databases."""

    networks: dict[str, dict] = field(default_factory=dict)  # region -> opaque network references for the vendor module
    service_account: str | None = None  # ServiceAccount of the Jobs (workload identity); None: the default one
    client_region_by_city: bool = False  # a database region without a network uses one of its Keeper city (OVH)


@dataclass(frozen=True)
class DbaasSettings:
    engine_version: str = "18"  # PostgreSQL major version to benchmark, when the database offers it (else the newest)
    create_minutes: int = 60  # the database must be created within this time
    sweep_after_hours: int = 6  # a stack without a live run this old is destroyed
    vendors: dict[str, DbaasVendor] = field(default_factory=dict)


@dataclass(frozen=True)
class Settings:
    """ConfigMap `k8s-inspector`, re-read every planning cycle."""

    planner_interval_minutes: int = 5
    auto_plan: bool = False
    bench_pool: str = "bench"  # the one NodePool every vendor shares
    bench_class: str = "bench"  # the SCNodeClass of that pool: names SCarpenter's offered-types ConfigMaps
    vendors: dict[str, VendorSettings] = field(default_factory=dict)
    defaults: Defaults = field(default_factory=Defaults)
    spot: SpotSettings = field(default_factory=SpotSettings)
    deadlines: Deadlines = field(default_factory=Deadlines)
    publish: PublishSettings = field(default_factory=PublishSettings)
    uploads: UploadSettings = field(default_factory=UploadSettings)
    dbaas: DbaasSettings = field(default_factory=DbaasSettings)
    from_configmap: bool = False  # False: the ConfigMap is missing or empty and these are the defaults


@dataclass(frozen=True)
class Config:
    """Everything a component needs: environment plus the current ConfigMap snapshot."""

    env: EnvConfig
    settings: Settings
