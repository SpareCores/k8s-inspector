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
planner:
  intervalMinutes: 5
  autoPlan: false                # true: the controller picks SKUs itself (below); false: only forced runs
vendors:
  oci:
    enabled: true
    autoPlan: false              # per-vendor override; needs publish
    maxInFlight: 2               # 0 pauses the vendor; unset: defaults.maxInFlight
    maxUsdPerHour: 2.0           # checked before a run starts
    capacity: SpotFirst          # SpotFirst (the default) | OnDemand
    publish: false               # true, or a list of SKUs: the publisher pushes these results to sc-inspector-data
defaults:
  maxInFlight: 2                 # conservative default for every vendor without its own maxInFlight
spot:
  maxSpotAttempts: 2
  maxLostMinutes: 60
  interruptionsToQuarantine: 2
  quarantineDays: 14
  spotMaxTaskMinutes: 240
deadlines:
  provisionCpuMinutes: 60
  provisionGpuHours: 6
  noOfferingMinutes: 10          # unschedulable anchor with no NodeClaim this long: -1 SCARPENTER_NO_OFFERING
  imagePullMinutes: 15
  holdIdleMinutes: 45
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

`deadlines.provisionCpuMinutes` / `provisionGpuHours` bound the wait for a node. A SKU SCarpenter cannot offer would
hold the vendor's slot for that whole time, so the controller ends it early: when the anchor Pod is still Pending
with `PodScheduled` False and a message, and no NodeClaim for its vendor and SKU exists `noOfferingMinutes` after the
attempt started (Karpenter creates one within seconds for any SKU it can launch), the remaining tasks end
`-1 SCARPENTER_NO_OFFERING: <PodScheduled message>`. This reads only the Pod, no extra RBAC; the detailed Karpenter
reason (Event `FailedScheduling`) is not used.

Only vendors listed with `enabled: true` are touched, and their results reach sc-inspector-data only with
`publish: true` or a list of SKUs (default `false`: runs and outputs happen, git stays untouched, and the runs of
SKUs that are not published are deleted after `keepRunHours`; their outputs stay in object storage and
`publish --rescan --sku` can publish them later). A vendor is enabled here when it is
removed from the legacy pipeline ([roadmap](../roadmap.md)); there is no shared routing file.

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
   minutes), in the legacy order: crc32 of `<vendor>/<sku>`, which mixes sizes. A SKU that has a `BenchmarkRun` (the
   lease, also while it is finished and not yet published) is skipped, and so is a SKU whose name is not a valid label
   value (the anchor selects the SKU by label; one warning per vendor and cycle) or whose `meta.json` in git is
   malformed (logged by the publisher). If Keeper lists no servers for the vendor, the planner logs it at info level.
4. A task of a SKU is due when Keeper does not block it (a block reason drops the task; the `keeper_gated` tasks are
   blocked by Keeper except on its allowlist, so they run only where Keeper allows them) and its
   `data/<vendor>/<sku>/schedule/<task>/meta.json` in sc-inspector-data `main` says so: no meta yet, or a
   `task_hash` different from the catalogue's (a changed task, a bumped `revision`, a `RERUN_` prefix), or a failure
   that is retried and whose wait has passed (`-1`: 2 h, or 24 h for `SCARPENTER_QUOTA_ZERO` and
   `SCARPENTER_NO_OFFERING`; `-4`: 2 h after the first, 6 h after the second). Anything else with the current hash is
   final: success, a task's own exit code above 0, `-3`, `-6`, `256` and `-5` ("exceeded retries") stay until the
   hash changes ([exit codes](exit-codes.md)). GPU tasks are dropped for a SKU whose `nvidia_smi` Keeper blocks (no
   GPU), and a SKU whose due tasks are only GPU tasks is not planned.
5. The first SKU in that order with at least one due task gets a run with exactly those tasks (not forced:
   Keeper's rules were applied). Then the caps are checked again; below them the next SKU follows.

The git state is read from the publisher's checkout (blobless, sparse: only the `schedule/*/meta.json` files of the
vendor), under the publisher's lock. If Keeper or git cannot be read, the cycle (or that vendor) plans nothing and
logs why: the planner never creates runs blindly. `k8s-inspector plan [--dry-run]` runs one cycle in its own
checkout; with `--dry-run` it creates nothing and prints the first SKU that would start for each vendor.

## CLI

`k8s-inspector serve | plan [--dry-run] | run <vendor> <sku> [--tasks …] [--force] [--on-demand] [--hold 2h] |
status [--failing] | cancel <run> | publish [--rescan --since 7d [--sku <sku>]…]`. Inside the cluster:
`kubectl -n inspector exec deploy/k8s-inspector -- k8s-inspector …`.

`cancel` only sets the annotation `inspector.sparecores.com/cancel: "true"` on the `BenchmarkRun` and returns; the
`serve` process ends the run in place (unfinished tasks `-4 CANCELLED`, Pods deleted, phase `Cancelled`) and keeps
the `BenchmarkRun` until the publisher purges it, so the CLI never races the runner. Deleting the `BenchmarkRun` with `kubectl` also stops the run, but drops
the record. `--sku` (repeatable, with `--rescan`) names a SKU that was never published, so its outputs are recovered too.
Every command logs JSON lines on stderr.

## Running instances per vendor

The bench NodePool has no `limits`: the goal is to benchmark every instance type of every vendor, so there is no pool-level
size or total bound. The cap on started and running instances of a vendor is `vendors.<v>.maxInFlight`
(default `defaults.maxInFlight`, 2; 0 pauses the vendor). The count is the larger of the runs that hold or are
acquiring a node (Pending, Provisioning, Running, Releasing, and a Failed or Cancelled run until its anchor Pod is
deleted) and the live NodeClaims of the vendor in the bench NodePool (NodeClaims carry the vendor in `spec.requirements` from creation and as the label `scarpenter.sparecores.com/vendor` after launch; anchors select the SKU by `scarpenter.sparecores.com/api-reference`, so SKUs whose names are not valid label values are refused; an instance can outlive its anchor). This is a
lower bound ("at least" that many instances); leftover NodeClaims without a holding run count as instances but do
not add to the spend. It applies to forced runs; [autoPlan](#autoplan) uses the same check before every run it creates.

`maxUsdPerHour`, when set, is checked before a run starts against the node prices of the holding runs. A run in
Pending or Provisioning has no node and no price yet, so while one exists a new run is refused ("wait until ... has
a price"). A run that has a node but no `scarpenter.sparecores.com/price` annotation counts as unknown spend: the
controller logs a warning and refuses the new run (the operator must check SCarpenter). When SCarpenter marks a
NodeClaim `price-incomplete: "true"`, the run records `status.priceIncomplete: true`; such a price understates the
spend, so the cap is checked against too low a sum.

The check is not atomic: two concurrent `run` commands for different SKUs can both pass it and exceed the limit by one.
The planner of `serve` is single-threaded, so autoPlan does not add to this race.
