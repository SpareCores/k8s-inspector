# Configuration

## Environment (Deployment)

Values are deployment configuration of the cluster platform, not part of this repository.

| Variable | Meaning |
|---|---|
| `K8SI_BUCKET`, `K8SI_BUCKET_REGION` | object storage bucket for outputs |
| `K8SI_ROLE_ARN` | IAM role assumed with the ServiceAccount token (web identity); its `MaxSessionDuration` must be at least 12 h (the controller asks for 12 h sessions); it needs `s3:DeleteObject` besides read, write, list and multipart actions under the run prefix |
| `K8SI_GIT_URL`, `K8SI_GIT_BRANCH` | optional: sc-inspector-data remote and branch (default `main`); without a URL nothing is published |
| `K8SI_GIT_KEY_FILE` | optional: mounted deploy key (Secret `k8s-inspector-git-key`); when the file is absent no `-i` is passed to ssh, and publishing to an SSH URL fails with a clear message |
| `KEEPER_API_BASE`, `KEEPER_API_KEY_FILE` | optional: Keeper API for eligibility (base default `https://keeper.sparecores.net`; key from Secret `k8s-inspector-keeper`; without the key file Keeper is called without a key) |
| `K8SI_NAMESPACE` | namespace of runs (default `inspector`) |
| `K8SI_SC_RUN_IMAGE` | the `sc-run` image of anchors and flush steps, digest-pinned (`ghcr.io/sparecores/k8s-inspector-sc-run@sha256:…`); `serve` and `run` refuse to start without it |
| `K8SI_DBAAS_IMAGE` | the DBaaS provisioner image of the Jobs, digest-pinned (`ghcr.io/sparecores/k8s-inspector-dbaas@sha256:…`); only DBaaS runs need it (`run --database` refuses without it, a DBaaS run ends `256` without it) |
| `K8SI_DBAAS_ROLE_ARN` | optional: IAM role of the provisioner Jobs for `dbaas-state/` (web identity of the ServiceAccount `k8s-inspector-dbaas`), limited to that prefix; the Jobs name it in an AWS profile `k8si-state` and set no `AWS_*` variable. Without it the Jobs get no state credentials from the controller |
| `K8SI_S3_ENDPOINT_URL` | optional S3-compatible endpoint, for object storage that is not AWS S3 (OCI) |
| `K8SI_KEY_PREFIX` | optional prefix of every object key, default empty (for example `k8s-inspector/spikes/` writes a first run under a scratch prefix); the bucket role must cover it ([output contract](output-contract.md)) |

The controller serves Prometheus metrics on port 9090 (`/metrics`, [monitoring](../explanation/monitoring.md)) and `/healthz`, which is the liveness probe.

## ConfigMap `k8s-inspector`

The platform creates and manages this ConfigMap (this repository does not ship it). The YAML is the value of key
`config.yaml`. The controller re-reads it through the Kubernetes API about every 30 s, so changes apply within
~30 s without a restart (the Deployment neither mounts nor hashes it, so a change never triggers a rollout). A
missing ConfigMap or an empty `config.yaml` gives the defaults, with no vendor enabled. An unknown key is logged as a
warning and ignored; an invalid value is an error and the controller keeps the last good settings.

```yaml
benchPool: bench                 # the one NodePool every vendor's bench nodes come from
benchClass: bench                # the SCNodeClass of that pool (names SCarpenter's offered-types ConfigMaps, see autoPlan)
planner:
  intervalMinutes: 5
  autoPlan: false                # true: the controller picks SKUs itself (below); false: only forced runs
vendors:
  oci:
    enabled: true
    autoPlan: false              # per-vendor override; needs publish
    maxInFlight: 2               # 0 pauses the vendor; unlimited: no limit; unset: defaults.maxInFlight
    maxUsdPerHour: 2.0           # checked before a run starts; unset: no cap
    maxNewRunsPerCycle: 10       # autoPlan: the most runs one cycle creates for the vendor (burst cap)
    capacity: SpotFirst          # SpotFirst (the default) | OnDemand | SpotUntilExhausted | SpotOnly (see Spot capacity modes)
    publish: false               # true, or a list of SKUs: the publisher pushes these results to sc-inspector-data
    dbaasAutoPlan: false         # autoPlan also creates managed-database runs for this vendor (see DBaaS autoPlan)
    dbaasMaxInFlight: 1          # DBaaS runs in flight; 0 pauses; counted over DBaaS runs only
    # dbaasMaxUsdPerHour: 3.0   # database + client price of the DBaaS runs in flight; unset (the default): no cap
defaults:
  maxInFlight: 2                 # conservative default for every vendor without its own maxInFlight; unlimited: no limit
spot:
  maxSpotAttempts: 2
  maxLostMinutes: 60
  interruptionsToQuarantine: 2
  quarantineDays: 14
  spotMaxTaskMinutes: 240
  quotaReserveAfterMinutes: 60   # autoPlan: a SKU deferred this long for busy spot quota reserves its buckets; 0: never (see Quota-aware planning)
  patienceDays: 14               # SpotUntilExhausted: a SKU is exhausted when its first counted try is this old ...
  coverage: 100                  # ... and counted tries fell in this percent of the week's slots (1-100)
  slotHours: 3                   # a slot is this many UTC hours of a weekday (1-24); 3 gives 56 slots a week
deadlines:
  provisionCpuMinutes: 60        # until a NodeClaim launches (a launch that keeps failing is ICE)
  provisionGpuHours: 6           # ... for a GPU SKU
  registrationMinutes: 8         # launched, not registered this long: BOOT_FAILURE
  registrationMinutesBareMetal: 14
  readyMinutes: 15               # registered, not Initialized (Ready) this long after registering: BOOT_FAILURE
  readyMinutesBareMetal: 30
  readyMinutesGpu: 45
  anchorReadyMinutes: 15         # node Initialized, anchor not Ready this long: ANCHOR_FAILED (temporary)
  noOfferingMinutes: 10          # unschedulable anchor with no NodeClaim this long: -1 SCARPENTER_NO_OFFERING
  noOfferingFastMinutes: 2       # ... the same when Karpenter's FailedScheduling event says the bench pool is incompatible
  imagePullMinutes: 15
  holdIdleMinutes: 45
dbaas:
  engineVersion: "18"            # PostgreSQL major version to benchmark when the database offers it, else its newest
  createMinutes: 60              # the database must be created within this time
  sweepAfterHours: 6             # a stack without a live run this old is destroyed
publish:
  intervalMinutes: 5
  keepRunHours: 24
uploads:
  partMiB: 16
  maxParts: 64
  singlePutMaxMiB: 32
  reputSeconds: 60
  urlTtlHours: 12
  resignEveryMinutes: 60
```

`deadlines.provisionCpuMinutes` / `provisionGpuHours` bound the wait until a NodeClaim of the run launches. After
launch, `registrationMinutes*` bound the time to registration (counted from the claim's creation, like Karpenter's own limit) and
`readyMinutes*` the time from registration to the NodeClaim's `Initialized` condition; a node that misses either ends
the run as `BOOT_FAILURE`. `anchorReadyMinutes` bounds the time from the node being Initialized (else from the anchor being
scheduled) to the anchor Pod being Ready (an image pull, a probe, a leftover taint); a miss ends the run as the temporary
`ANCHOR_FAILED` ([launch retries](../explanation/launch-retries.md#a-node-that-does-not-come-up)).

<a id="deadlines"></a>The class of a SKU picks the limits. Bare metal is told by the SKU name: OCI `BM.`, OVH `bm-`,
AWS `.metal`, GCP `-metal`, Alibaba `ecs.ebm`, Vultr `vbm` and `vdm` (`BARE_METAL_SKU` in `runner.py`; some Vultr
`vcg` GPU plans are metal, but the name does not say which, so GPU SKUs get the bare-metal registration limit). A GPU SKU is one with a GPU
task in the run or accelerator labels (`scarpenter.sparecores.com/accelerator-count`, `accelerator-manufacturer`) on
its NodeClaim; a GPU node gets `readyMinutesGpu` even when it is also bare metal.

Karpenter itself deletes a NodeClaim that has not registered 15 minutes after its creation; that limit is a constant
in Karpenter. `registrationMinutes*` must stay below it, or the claim is gone before the controller acts (the run then
still ends `BOOT_FAILURE`, because it remembers the claim, but only after the fact).

Upgrade: apply the BenchmarkRun CRD before the controller. With the old CRD the API server prunes
`status.uninitializedClaims` and the detection of a claim that vanished is off. A SKU SCarpenter cannot offer would
hold the vendor's slot for that whole time, so the controller ends it early: when the anchor Pod is still Pending
with `PodScheduled` False and a message, and no NodeClaim for its vendor and SKU exists `noOfferingMinutes` after the
attempt started (Karpenter creates one within seconds for any SKU it can launch), the run ends as a temporary
`SCARPENTER_NO_OFFERING: <PodScheduled message>` ([launch retries](../explanation/launch-retries.md); a forced run records it as `-1`). When the most recent Karpenter `FailedScheduling` Event of the anchor already has
a `no instance type met all requirements` clause for `karpenter.sh/nodepool In [<benchPool>]` (and the attempt has no launch failure), the run ends the same way (`no instance type met all requirements for <SKU> in nodepool <benchPool>`)
after `noOfferingFastMinutes`; this needs `list` on `events`.

Karpenter replaces a NodeClaim that fails to launch with a new one every few minutes, so the deadlines above would
not fire. A failure shows as a `Launched` condition with reason `LaunchFailed` (status Unknown or False), except
InsufficientCapacity (also what SCarpenter returns when every candidate failed or had no quota): Karpenter deletes
that claim at once, with no condition, and only publishes an `InsufficientCapacityError` Event on the anchor Pod
(`NodeClaim <name> nominated for this pod failed to launch: <message>`; needs `list` on `events` and Karpenter v1.15
or newer). That message carries the last candidate's class, or none (quota headroom, no candidate), and the controller
treats it as `ICE` (`QUOTA` for a quota class). Detection can lag up to the 300 s resync, so a 3rd or 4th ICE claim may
be tried before the cap applies; ICE bills nothing. The controller remembers each failed NodeClaim of the attempt,
from the condition or from that Event, in `status.launchFailures` and, at the second distinct one (or when the
provisioning pre-launch budget passes after a failure), ends the run with the reason of the class SCarpenter put in the last message
([launch retries](../explanation/launch-retries.md)) and deletes the anchor; when the attempt allowed spot and the
reason is `LAUNCH_REJECTED`, it instead ends `LaunchFailed` and the next attempt is on-demand only
([spot](../explanation/spot.md)). A single transient failure is tolerated, but not forever: once an attempt has a launch
failure, no claim is pending and `noOfferingMinutes` have passed, a run ends with the last failure's reason when
SCarpenter's offered-types listing is readable and does not offer the SKU ([Offered types](#offered-types)); with an
unreadable or stale listing it keeps waiting for the pre-launch budget.

A run in Pending or Provisioning also stops when its SKU is no longer eligible in Keeper: not ACTIVE any more, or every
unfinished server task of the run blocked. The controller asks Keeper (the planner's cached answer, one instance shared
by both) at most every 10 minutes per run and deletes the run's NodeClaims. A run that never reached a node and has no
finished task is deleted and records nothing. A run with a finished task or a node (a spot-interrupted run back in
Pending) ends Failed instead: its unfinished tasks end `-1`, and the finished ones are published. A DBaaS run and a
forced run are never checked, and an unreachable Keeper changes nothing.

Only vendors listed with `enabled: true` are touched, and their results reach sc-inspector-data only with
`publish: true` or a list of SKUs (default `false`: runs and outputs happen, git stays untouched, and the runs of
SKUs that are not published are deleted after `keepRunHours`; their outputs stay in object storage and
`publish --rescan --sku` can publish them later). A vendor is enabled here when it is
removed from the legacy pipeline ([roadmap](../roadmap.md)); there is no shared routing file.

## DBaaS

`dbaas.vendors` holds what the cluster platform provides per vendor ([add a DBaaS vendor](../how-to/add-dbaas-vendor.md#what-the-platform-provides));
it is empty by default, which leaves DBaaS off (the sweeper still lists the stacks in the bucket and reports them):

```yaml
dbaas:
  vendors:
    gcp:
      serviceAccount: k8s-inspector-dbaas-gcp   # optional: workload identity of the Jobs (default k8s-inspector-dbaas)
      clientRegionByCity: false                  # optional (default false): see below
      networks:                                  # client region -> what the vendor module needs, passed through unchanged
        europe-west4:
          network: projects/p/global/networks/dbaas
          subnet: projects/p/regions/europe-west4/subnetworks/dbaas
```

The values under `networks.<region>` are infrastructure outputs of the platform, opaque to the controller; only the
vendor module reads them. The key is the region of the client (bench) node, which is the database's region unless the
vendor's database regions are not its server regions (OVH). `clientRegionByCity: true` is for such a vendor only: a
database region without a `networks` entry then uses the client region of the same Keeper city (country and city) that
has one. Without it (the default) a database region without an entry is not used, so a missing entry never pairs a
database with another region's network. A region without an entry is never used as a client region for a database of
that vendor. The Jobs take the
vendor's cloud credentials from the optional Secret `k8s-inspector-dbaas-<vendor>`, and the stack passphrase from the
Secret `k8s-inspector-dbaas-state` (`passphrase`).

## autoPlan

With `autoPlan` on (`planner.autoPlan`, overridable per vendor) for an enabled vendor, every `planner.intervalMinutes`
the controller starts runs by itself; there is no batching or waiting for a full billed hour, work starts as soon as
it is due and the vendor has room. Per vendor:

1. The vendor must publish (`publish: true` or a list; only the listed SKUs are planned). Without publishing, git
   would never learn that a task ran and the SKU would run again and again; the planner logs a warning and skips the
   vendor.
2. If the vendor has a run in phase Publishing with a finished task that stayed unpublished for more than twice
   `publish.intervalMinutes` (pushes keep failing), the planner logs a warning and skips the vendor: new runs would
   only pile up unpublished. If one more run would pass `maxInFlight` or `maxUsdPerHour` ([below](#running-instances-per-vendor)) the vendor is
   skipped before anything else is read. `maxInFlight: 1` means one instance at a time.
3. The candidate SKUs are the ACTIVE servers of the vendor that Keeper lists
   (`/inspector/servers/task_block_reason_codes`, the same call that answers eligibility; the answer is cached for 30
   minutes), ranked by popularity ([Order and packing](#order-and-packing)). A SKU that has a `BenchmarkRun` (the
   lease, also while it is finished and not yet published) is skipped, and so is a SKU whose name is not a valid label
   value (the anchor selects the SKU by label; one warning per vendor and cycle) or whose `meta.json` in git is
   malformed (logged by the publisher). If Keeper lists no servers for the vendor, the planner logs it at info level.
   A SKU that SCarpenter does not offer is skipped too, and so is the whole vendor when SCarpenter's listing is
   missing, unreadable or stale (see [Offered types](#offered-types)); forced runs are never filtered.
4. A task of a SKU is due when Keeper does not block it (a block reason drops the task; the `keeper_gated` tasks are
   blocked by Keeper except on its allowlist, so they run only where Keeper allows them) and its
   `data/<vendor>/<sku>/schedule/<task>/meta.json` in sc-inspector-data `main` says so: no meta yet, or a
   `task_hash` different from the catalogue's (a changed task, a bumped `revision`, a `RERUN_` prefix), or a failure
   that is retried and whose wait has passed (`-1`: 2 h, 30 days
   for `SCARPENTER_LAUNCH_REJECTED` and `SCARPENTER_BOOT_FAILURE`, and no wait for the metas of temporary
   reasons, which are paced by the launch ConfigMap; `-4`: 2 h after the first, 6 h after the second). Anything else with the current hash is
   final: success, a task's own exit code above 0, `-3`, `-6`, `256` and `-5` ("exceeded retries") stay until the
   hash changes ([exit codes](exit-codes.md)). GPU tasks are dropped for a SKU whose `nvidia_smi` Keeper blocks (no
   GPU), and a SKU whose due tasks are only GPU tasks is not planned.
   A task is also due (`missing from the newest hardware variant`) when the SKU's newest hardware variant, the
   highest-numbered directory `data/<vendor>/<sku>/<N>/` (0 included), has no `<task>/meta.json`, Keeper allows
   the task, no rule above applies, and the task's latest attempt on the SKU started before the hardware
   appeared: `first_seen` in `<N>/variant.json`. The latest attempt is `last_attempt` of
   `schedule/<task>/meta.json` ([output contract](output-contract.md#last_attempt)), else the meta itself; a task
   without a schedule meta is due as never run. A node on new hardware makes the
   publisher create a new variant, which would otherwise hold only the tasks that happened to be due. The planner
   cannot choose the hardware, so a fill may land on an older variant, fail or be lost and leave `N` as it was.
   After an attempt since `first_seen`, only the retry rules above decide, applied to that attempt wherever it
   landed: `-4` and a command that does not start are retried after their backoff and end as `-5` after three in a
   row, a permanent launch failure (`LAUNCH_REJECTED`, `BOOT_FAILURE`) is re-checked after 30 days, a task's own exit
   code and the other final codes are not repeated, a temporary launch failure leaves nothing in git and is paced by the launch backoff. A
   variant without a readable `first_seen` is not filled (a missing `variant.json` is logged at debug level, an
   unreadable `first_seen` at warning level), nor is a task whose latest attempt has no `start`. The state is all
   in git; nothing is kept in the cluster.
   A SKU waiting in the launch ConfigMap (`next` ahead) or unavailable in SCarpenter's offered-types ConfigMap is
   skipped ([launch retries](../explanation/launch-retries.md), [Offered types](#offered-types)).
   A SKU whose quota is busy is deferred to a later cycle ([Quota-aware planning](#quota-aware-planning)).
5. Each SKU in that order with at least one due task gets a run with exactly those tasks (not forced:
   Keeper's rules were applied), unless it does not fit; then the next one is tried ([Order and packing](#order-and-packing)).
   The cycle ends when `maxInFlight` is reached, when the `maxUsdPerHour` cap is reached, after `maxNewRunsPerCycle`
   new runs, or when no candidate is left.

The git state is read from the publisher's checkout (blobless, sparse: only the `schedule/*/meta.json` files of the
vendor), under the publisher's lock; the schedule metas, the newest variants with their tasks, and `first_seen` of a variant (only when a candidate task is
missing from it) are three reads in separate lock sections, the last two from the git trees with no checkout (origin only moves forward; a publish landing between the reads is covered by the SKU lease, which keeps two runs off the same SKU). If Keeper or git cannot be read, the cycle (or that vendor) plans nothing and
logs why: the planner never creates runs blindly. `k8s-inspector plan [--dry-run]` runs one cycle in its own
checkout (without the popularity cache of `serve`, so it orders by crc32); with `--dry-run` it creates nothing and prints the first SKU that would start for each vendor.

### Order and packing

**Order.** The candidates are ranked from Keeper data (`popularity.rank`; no static table of series, families or regions):

1. SKUs with accelerators (`accelerator_count > 0`) come last.
2. The others by the **region breadth of their series**: the most regions with an active on-demand price that any SKU of
   the series has. Where breadth means nothing, the cheapest on-demand price goes first instead: a vendor whose priced
   regions are at most 3 (OCI), and a SKU whose series Keeper does not know (UpCloud, some Vultr). Breadth-ranked SKUs
   precede price-ranked ones.
3. Ties: a newer `memory_generation` (the number in the string, `DDR5` before `DDR4`), a series that is started already
   (a SKU of it has a `BenchmarkRun`, or a result with a `run_id` in git), the series itself (its SKUs stay together),
   the smaller SKU (vCPUs, then memory), then crc32 of `<vendor>/<sku>`.

The log line `planned run` carries the components (`score`) and the `regions` the run is confined to. A SKU without Keeper data is not blocked:
it follows the ranked ones, in crc32 order, and a cycle without any data (Keeper down, a cold cache) is the crc32 order
of earlier releases.

**Keeper load.** The data is one `/v2/server/<vendor>/<sku>` and one `/server/<vendor>/<sku>/prices` call per SKU, plus
one `/regions` call per vendor, kept for 24 hours (stale data is used until the SKU is asked again). Only SKUs that have a
due task are asked, in crc32 order, one call at a time, at most 40 SKUs per cycle over all vendors together. The first
failure starts a 10-minute cooldown in which no vendor asks Keeper, and the SKU it happened on is not asked again for 6
hours. A cold cache over vendors with hundreds of due SKUs is therefore ranked partly and filled in over several hours.
The breadth of a series is the maximum over the SKUs asked so far. There is never a bulk or unbounded price query.

**Fill, not stop.** The cycle goes through the ranked SKUs and *skips* one that does not fit instead of stopping: a SKU
deferred for quota, or whose node (Keeper's price for the capacity type and region it would use) costs more than what is
left under `maxUsdPerHour`, leaves room for smaller ones behind it. A run planned in the cycle counts with that price
estimate until it has a real price annotation, so the budget fits several runs in one cycle; runs of earlier cycles
without a price still make the cap wait. A run that may use spot is estimated at the dearer of its spot and on-demand
price, because spot falls back to on-demand. With `maxUsdPerHour` set, a run created without any price estimate (Keeper
has no price) ends the cycle for the vendor: the next run waits until it has a real price. `maxInFlight` and
`maxUsdPerHour` are both optional: unset `maxUsdPerHour` is no cap, `maxInFlight: unlimited` is no limit, and neither
requires the other (the operator is responsible for what an unbounded vendor costs). `maxInFlight: unlimited` works
under `defaults` and under a vendor; a vendor's `maxInFlight` unset (or `null`) inherits the default. `maxNewRunsPerCycle`
(default 10) bounds the runs one cycle creates per vendor. With `--dry-run` the first SKU of a vendor is returned.

**Regions.** See [Quota-aware planning](#quota-aware-planning): where SCarpenter lists regional quota, the run is
confined to the regions where it fits.

### DBaaS autoPlan

`vendors.<vendor>.dbaasAutoPlan: true` (default `false`, and the vendor must be `enabled` and publish) lets autoPlan also
create DBaaS runs for the vendor, in their own cycle every `planner.intervalMinutes` (`k8s-inspector plan` runs it too).
It is independent of `planner.autoPlan` and of `autoPlan` of the vendor: server and DBaaS planning are switched
separately. Per vendor:

1. The vendor must have networks in `dbaas.vendors.<vendor>`, be below `dbaasMaxInFlight` (default 1; the runs that hold
   a client node or a database not yet destroyed) and must not be launch-paused. A database is skipped when the running
   DBaaS runs plus its own price (database plus client, from Keeper) would pass `dbaasMaxUsdPerHour` (default: no cap).
   Server runs and the client node do not count against `maxInFlight` or `maxUsdPerHour`, and server runs do not count
   against the DBaaS limits. At most one DBaaS run per vendor is created per cycle.
2. The candidate databases are the ACTIVE PostgreSQL databases Keeper lists for the vendor that Keeper does not block for
   the DBaaS task (`/inspector/databases/task_block_reason_codes`, cached for 30 minutes), whose results may be published
   (`publish` true or listing the database's id), in the order crc32 of `<vendor>/<database>`.
3. A database is due by the same rules as a server task (step 4 above), read from
   `dbaas/<vendor>/<id>/postgres/<version>/standalone/<task>/meta.json`; the version is `dbaas.engineVersion` when the
   database offers it, else its newest. A malformed meta skips the database.
4. The first due database gets a run in its cheapest database region that has a client region (the database region of the
   last failed attempt last): the region itself or, for a vendor with `clientRegionByCity`, one of the same Keeper city, with a `networks` entry. The client is the
   one the picker chooses there ([DBaaS](../explanation/dbaas.md#which-database-where-with-which-client)). A database
   without such a region or client is skipped.

If Keeper or git cannot be read the cycle (or that vendor) plans nothing and logs why.

### Offered types

SCarpenter publishes which instance types it can launch in ConfigMap `scarpenter-offered-<benchClass>-<vendor>`
(for example `scarpenter-offered-bench-oci`) in namespace `scarpenter-system`, as gzipped JSON in `binaryData` key
`types.json.gz` (an older SCarpenter writes plain JSON in `data` key `types.json`, which is the fallback when the
gzip key is absent; the gzip wins when both exist). Once per vendor and cycle autoPlan reads it (the controller may `get` only the ConfigMaps named in `deploy/controller/rbac.yaml`: the default `bench` class of every SCarpenter vendor, so a non-default `benchClass` or a new vendor needs that list updated) and skips a
SKU, matched by the entry's `apiReference` (entries without one are ignored; of two with the same one the available
wins), when:

- its entry is missing or `available` is false (e.g. a tenancy quota; a type is unavailable only when SCarpenter has
  marked or left out all of its offerings), or
- the entry has `available: true` but no `capacityTypes`, or
- the vendor's `capacity` is `OnDemand` and `on-demand` is not in the entry's `capacityTypes` (`SpotFirst` takes
  either type).

One info line per vendor and cycle gives the count and a few examples with SCarpenter's `reason`. Such SKUs would
only end in a `NO_OFFERING` launch failure after the fail-fast wait.

The filter fails closed: when the ConfigMap is missing, unreadable, malformed or stale (`generatedAt` older than 30
minutes: SCarpenter stopped writing it), nothing is known about the SKUs, so the planner logs a warning and plans
nothing for that vendor in that cycle. Forced runs are not affected. A running provisioning treats such a listing the
same way: no answer, keep waiting.

Every `available: false` entry is skipped; SCarpenter rewrites the entry when a mark expires. The `reason` and
`offerings[].until` (RFC3339) of an offering that a launch error marked (for example `DailyLimit`, until the next
00:00 UTC) do not change that: the `until` of a SKU, the latest among its unavailable offerings, only sets how long
the [launch backoff](../explanation/launch-retries.md#temporary-failure) of a `DAILY_LIMIT` failure waits.

The filter fails open: a ConfigMap that is absent (the vendor is not opted in), unreadable, malformed or stale
(`generatedAt` older than 30 minutes) or holds a corrupt gzip filters nothing, and the cycle logs why. A ConfigMap
without `truncated` counts as complete; the optional `zonesOmitted` field is ignored (nothing reads zones). In a ConfigMap with
`truncated: true` only entries with `available: false` are skipped; a missing entry proves nothing.

### Spot capacity modes

The vendor's `capacity` decides which capacity types a run's anchor allows ([spot](../explanation/spot.md#spot-capacity-modes)):

| `capacity` | Anchor |
|---|---|
| `SpotFirst` (default) | `spot, on-demand`: Karpenter launches on-demand by itself when no spot node can launch |
| `OnDemand` | `on-demand` |
| `SpotUntilExhausted` | `spot` only, until the SKU is exhausted; then like `SpotFirst`. Also `spot, on-demand` at once when spot is impossible for the SKU, and `on-demand` after `maxSpotAttempts` interruptions, for quarantined SKUs and for tasks that do not allow spot, as in `SpotFirst` |
| `SpotOnly` | `spot` only, every attempt |

autoPlan applies the spot-only modes per SKU, from the SKU's entry in the offered-types listing. **Spot is impossible**
for a SKU when none of its offering groups, available or not, is a spot one (a group SCarpenter marks `NotLaunchable`
does not count), or its spot group never fits (`maxLimit < need`). An available spot offering without quotas is
possible, and so is an entry that lists no offering groups (an older SCarpenter says nothing).

- `SpotOnly` does not plan a SKU when spot is impossible, `spot` is not among the entry's `capacityTypes` (spot is
  unavailable at the moment), its due tasks do not allow spot, it is quarantined, or its spot launch was rejected
  (below): it never runs on-demand. A running `SpotOnly` attempt also needs `spot` in `capacityTypes` for its ICE
  launch failures to go on (the usual rule needs only an available entry).
- `SpotUntilExhausted` plans a SKU only in a slot it has not tried yet (below). Where on-demand is allowed at once, or
  the SKU is exhausted, it is planned like `SpotFirst` (each such plan logs `... exhausted, on-demand allowed` for an
  exhausted SKU). A rejected spot launch (`LAUNCH_REJECTED`, an offering the cloud does not support) and
  `MAX_SPOT_RECLAIMS` lost spot claims send the next attempt to on-demand, as in `SpotFirst`.
- A `SpotOnly` attempt whose spot launch is rejected for the SKU means spot is impossible for it: the attempt closes
  `SpotRejected`, the run ends without results (no `-1`, the tasks stay due) and the SKU is not planned under `SpotOnly`
  for 30 days (the key `rejected--<vendor>.<sku>` of the ConfigMap `k8s-inspector-spot`; a spot node that launches
  clears it). The controller logs a warning.

A **slot** is `slotHours` UTC hours of one weekday (`(weekday, hour // slotHours)`, 56 a week at 3 hours). A **try** is an
attempt launched spot-only that found no spot capacity in the regions the run was confined to (those where the spot
quota fitted) while their spot quota had room: the run ends `SpotUnavailable` ([run records](run-records.md)). It counts
only with evidence of lack of capacity: the error's `classes:` summary or a launch error names `InsufficientCapacity` or
`ZoneUnusable` (mixed with rejection classes it still counts). SCarpenter words a spot offering the cloud refuses
(`classes: Unsupported:3`, also `InvalidOffering`, `Fatal`, `NotFound`) as insufficient capacity too; that is a
rejection (below), not a try. An
attempt whose launch was refused for quota, that ended with `no candidate offering had quota headroom`, or whose spot
quota showed no room for the run's regions when it failed, is not a try (`quota_busy`, the usual launch deferral
applies), and neither is a provisioning deadline without any evidence. The ConfigMap `k8s-inspector-spot` keeps one
small value per SKU under `tries--<vendor>.<sku>`: the first and last try, their count and the set of slots they fell
in (never a list of tries). A value whose last try is older than twice `patienceDays` counts as none and is replaced by
the next try; a spot node that launches removes the keys. Losing the ConfigMap is harmless: spot is tried again. While
the ConfigMap cannot be read the planner skips the SKUs of these modes (it is read once per cycle), and a try that
cannot be written is not counted (the usual launch deferral applies instead), so nothing loops.

- A SKU is **due** for planning in a slot with no try. When every slot of the week has a try, a SKU is due once a day.
  A SKU that stays unavailable therefore costs one run, one run record and up to the provisioning wait per day
  under `SpotOnly`, and until its patience is over under `SpotUntilExhausted`.
- A SKU is **exhausted** (`SpotUntilExhausted` only; `SpotOnly` never is) when its first counted try is at least
  `patienceDays` old and counted tries fell in at least `coverage` percent of the week's slots. It stays exhausted
  (its runs may use on-demand) while its value exists: that is until a spot node launches or its last try is older
  than twice `patienceDays`, after which spot is tried afresh.

Each counted try logs one line, for example `x8i.96xlarge: 41/56 slots tried, spot available <7% (95%)`: the share of
tries that would find spot is below the bound with 95% confidence (`1 - 0.05^(1/n)` for `n` failed tries). The
structured `spot_try` line and the metrics are in [monitoring](../explanation/monitoring.md).

### Quota-aware planning

Each offering group in `offerings[]` of the listing (one per capacity type, available or not) may carry
`quotas: [{bucket, unit, need, maxLimit, maxHeadroom, regions}]`: the quota buckets one instance of the type consumes (`need`
is SCarpenter's number, the planner does not compute vCPUs) and the largest limit and the largest current headroom
(limit - used - reserved) among the group's regions and zones. A group without the field, or with a damaged one, has no
quota model and the planner behaves as without this section; so does a listing that is missing, stale or truncated
(see above; a truncated listing proves nothing about quota, so none of it is used). Per entry, **busy** means
`maxLimit >= need > headroom` and **never fits** means `maxLimit < need`; a group never fits when any entry does, else
it is busy when any entry is.

`maxLimit` is published promptly, so "never fits" is exact. `maxHeadroom` is only a hint: SCarpenter refreshes it when
availability or a limit changes and at least every 10 minutes, so it can be that old (the listing's `generatedAt`), it
leaves out regions whose quota data is unknown or stale, and a bucket that is global to the vendor shares its headroom
across regions. SCarpenter's quota gate decides at launch. `maxHeadroom` belongs to its SKU (the maximum over that
SKU's regions), so each SKU is tested against its own value.

For a SKU with due tasks the planner picks the group the run will use and decides:

- A run that may use spot (the vendor is not `OnDemand`, its due tasks allow spot, `spot.spotMaxTaskMinutes`, and the SKU is
  not quarantined, [spot](../explanation/spot.md)) uses the spot group. If it is busy the SKU is **deferred**: no run is
  created this cycle, because the run would fall back to on-demand and cost several times as much; the planner logs
  `autoPlan defers <vendor>/<sku>: spot quota busy (bucket B: need N vCPU, headroom H)`. If the spot group has room the run is
  planned. If it never fits, or the SKU has no spot quotas, the on-demand group decides.
- Any other run (`OnDemand` vendor, on-demand-only tasks, quarantined SKU) uses the on-demand group: busy means
  deferred, a group that never fits or has no quotas changes nothing.

A regional bucket also lists `regions: [{region, limit, headroom}]`, the regions with known, fresh data (it is as old as
`maxHeadroom`); a global (account-wide) bucket has no `regions` list, because `maxLimit` and `maxHeadroom` are already the
exact account figures. A list that is absent or damaged is ignored for that entry.

**Placement and pinning.** When an entry of the chosen group has `regions`, the candidate regions are those that every
such entry lists, narrowed to the regions that the zones of the capacity type's available offering groups name (all its groups when none is
available): a zone belongs to a region when it is the region, or the region plus a letter (`us-east-1a`) or a dash and a
letter (`europe-west1-b`). When no zone names a candidate region (zone names like Azure's `1` say nothing) the set stays. A region is out when the run does not fit one of the entries there (its regional `headroom`) or a
global entry of the group (`maxHeadroom`), after what is consumed in the cycle, or when another SKU reserves the quota
there (below). The run is confined to **all** the others: `spec.regions` lists them, and the anchor of every attempt
requires `topology.kubernetes.io/region In [...]` in its node affinity. Karpenter and SCarpenter pick an available
offering among them, so a region with an ICE mark or a daily limit does not starve the SKU. The quota accounting counts
the run against one expected landing region: the cheapest of them by Keeper's price for the capacity type the group stands
for (the spot price for the spot group, else on-demand), and without any price the one with the most room.
With no candidate left the SKU is deferred as busy (`autoPlan defers <vendor>/<sku>: spot quota busy (bucket B: need N
vCPU, headroom H in <region> (and K more regions))`). Without `regions` (an older SCarpenter, a global bucket, unknown
regions) the run is not confined and the headroom is `maxHeadroom` minus everything consumed in the bucket, as before. The
set is chosen once, when the run is created, and stays for all attempts of the run (a spot interruption or an on-demand
fall-back after a failed launch is retried within the same set). A run that ends without a node is final like any run,
and the SKU's next run (after the launch backoff) gets a set computed from the listing of that time.

The headroom of a SKU's bucket is its `maxHeadroom` (a regional entry: the region's `headroom`) minus what the vendor's own runs consume that the listing does not
show yet, per bucket and region. A run counts in the region of its node, else in its only region; an unconfined run (a forced
run, a run created before regions were planned, a SKU without region data) and one confined to several regions that has
no node yet count against every region of a regional bucket, and such a run does not start a drain. That is every run planned in the cycle (so several runs in one cycle cannot overshoot a quota), and every
server or DBaaS client run in phase Pending, Provisioning or Running that was created after the listing's `generatedAt`
(planned in an earlier cycle since then) or that has neither a NodeClaim nor a node (SCarpenter's `reserved` ledger
starts with a launch, so an anchor without a NodeClaim is not in it). A run with a NodeClaim that is older than
`generatedAt` is in `reserved` or `used` already and is not counted again; a NodeClaim created just before
`generatedAt` may not be in `reserved` yet, so the headroom can be over-stated. A run counts with the capacity it will
use (`spot.capacity_for_next_attempt`: its tasks, attempts and the SKU's quarantine, so an on-demand-only run after a
failed launch counts against the on-demand group). A run that may use spot counts against the spot group only (a spot offering without quotas counts nothing, for planned and counted runs alike): the
fall-back to on-demand when spot fills before the launch is not planned for. A DBaaS client run counts, with the
on-demand group, once its client SKU is known. Nothing of this is read when no SKU of the listing has `quotas`.

A big SKU would starve behind smaller SKUs that keep taking the bucket. A SKU is **big** in a region when its `need` is more than
half of an entry's limit (the region's `limit`, else `maxLimit`). The controller remembers since when each big SKU is
deferred for a busy spot quota (in memory; a restart resets it), with the bucket keys it asks to drain (none while it is
not big in the region it chose; its waiting time does not depend on the region), and rebuilds that
memory every cycle: a SKU that
is not deferred as spot-busy again (planned, no longer due, leased, no longer listed by Keeper, not offered) is
forgotten, and one that is held back by another SKU's reservation keeps its time, as do the SKUs after the point where a cap (`maxInFlight`, `maxUsdPerHour`, `maxNewRunsPerCycle`) stopped the cycle. A big SKU deferred for longer than
`spot.quotaReserveAfterMinutes` (default 60; `0` turns the reservation off) **reserves** its buckets in ONE region (**drain**):
the region it chose when it started waiting, kept while it stays a candidate. That is the region with the most room among
those where the vendor's own runs hold the bucket, so the runs in flight can drain. No other run that would consume the
bucket is placed in that region (the other regions keep filling), and a SKU that fits nowhere else is deferred (`autoPlan
defers <vendor>/<sku>: its quota is reserved for <sku>`) until the big SKU is planned or stops waiting. The reservation
applies only while the vendor's own runs in flight consume the bucket in that region: when other tenants or instances
hold the quota, keeping SKUs back would free nothing. Without `regions` the bucket as a whole is reserved, as before. Of
two reserving SKUs the longer waiter wins.

Residual race: a run planned while spot fit can still end on on-demand when Karpenter finds spot full at launch time
(another consumer took the quota in between); the planner does not prevent it.

The listing only carries `quotas` (and `regions`) from the SCarpenter releases that add them; before that nothing here applies.

## CLI

`k8s-inspector serve | plan [--dry-run] | run <vendor> <sku> [--tasks …] [--force] [--on-demand] [--hold 2h] |
run <vendor> --database <id> [--region <region>] [--client-region <region>] [<client sku>] [--force] [--hold 2h] |
status [--failing] | cancel <run> | publish [--rescan --since 7d [--sku <sku>]… | --backfill-runs --since 120d | --emit-cost-logs --since 120d]`. Inside the cluster:
`kubectl -n inspector exec deploy/k8s-inspector -- k8s-inspector …`.

`cancel` only sets the annotation `inspector.sparecores.com/cancel: "true"` on the `BenchmarkRun` and returns; the
`serve` process ends the run in place (unfinished tasks `-4 CANCELLED`, Pods and the run's NodeClaims deleted, so a node that is still booting is removed too, phase `Cancelled`; the
publisher writes nothing for those tasks, so the previous results stay) and keeps the `BenchmarkRun` until the publisher purges it, so the CLI never races the runner. Deleting the `BenchmarkRun` with `kubectl` also stops the run, but drops
the record. `--sku` (repeatable, with `--rescan`) names a SKU that was never published, so its outputs are recovered too. `--backfill-runs` writes the [run records](run-records.md#backfill) of past runs.
Every command logs JSON lines on stderr.

## Running instances per vendor

The bench NodePool has no `limits`: the goal is to benchmark every instance type of every vendor, so there is no pool-level
size or total bound. The cap on started and running instances of a vendor is `vendors.<v>.maxInFlight`
(default `defaults.maxInFlight`, 2; 0 pauses the vendor; `unlimited` is no limit). The count is the larger of the runs that hold or are
acquiring a node (Pending, Provisioning, Running, Releasing, and a Failed or Cancelled run until its anchor Pod is
deleted) and the live NodeClaims of the vendor in the bench NodePool (NodeClaims carry the vendor in `spec.requirements` from creation and as the label `scarpenter.sparecores.com/vendor` after launch; anchors select the SKU by `scarpenter.sparecores.com/api-reference`, so SKUs whose names are not valid label values are refused; an instance can outlive its anchor). This is a
lower bound ("at least" that many instances); leftover NodeClaims without a holding run count as instances but do
not add to the spend. It applies to runs started without `--force` ([forced runs](../how-to/run-forced.md) skip it but count toward it); [autoPlan](#autoplan) uses the same check before every run it creates.

`maxUsdPerHour`, when set, is checked before a run starts (forced runs skip it) against the node prices of the holding runs. A run in
Pending or Provisioning has no node and no price yet, so while one exists a new run is refused ("wait until ... has
a price"); autoPlan counts a run it planned in the same cycle with the Keeper price estimate instead. A run that has a node but no `scarpenter.sparecores.com/price` annotation counts as unknown spend: the
controller logs a warning and refuses the new run (the operator must check SCarpenter). When SCarpenter marks a
NodeClaim `price-incomplete: "true"`, the run records `status.priceIncomplete: true`; such a price understates the
spend, so the cap is checked against too low a sum.

Without `--force` the check is not atomic: two concurrent `run` commands for different SKUs can both pass it and exceed the limit by one.
The planner of `serve` is single-threaded, so autoPlan does not add to this race.
