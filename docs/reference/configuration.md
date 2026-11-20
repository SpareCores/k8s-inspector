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
  autoPlan: false                # true: plan from Keeper and git; false: only forced runs
vendors:
  oci:
    enabled: true
    autoPlan: false              # per-vendor override
    maxInFlight: 2               # 0 pauses the vendor; unset: defaults.maxInFlight
    maxUsdPerHour: 2.0           # checked before a run starts
    capacity: SpotFirst          # SpotFirst (the default) | OnDemand
    publish: false               # true, or a list of SKUs: the publisher pushes these results to sc-inspector-data
defaults:
  maxInFlight: 2                 # conservative default for every vendor without its own maxInFlight
batching:                        # hourly-billed vendors: start when expected work fills enough of the billed hour
  hourlyMinFillRatio: 0.5
  hourlyMaxWaitDays: 14
  perSecondMinWorkMinutes: 10
  perSecondMaxWaitDays: 3
spot:
  maxSpotAttempts: 2
  maxLostMinutes: 60
  interruptionsToQuarantine: 2
  quarantineDays: 14
  spotMaxTaskMinutes: 240
deadlines:
  provisionCpuMinutes: 60
  provisionGpuHours: 6
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

Only vendors listed with `enabled: true` are touched, and their results reach sc-inspector-data only with
`publish: true` or a list of SKUs (default `false`: runs and outputs happen, git stays untouched, and the runs of
SKUs that are not published are deleted after `keepRunHours`; their outputs stay in object storage and
`publish --rescan --sku` can publish them later). A vendor is enabled here when it is
removed from the legacy pipeline ([roadmap](../roadmap.md)); there is no shared routing file.

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
deleted) and the live NodeClaims of the vendor in the bench NodePool (NodeClaims carry the label `scarpenter.sparecores.com/vendor`; an instance can outlive its anchor). This is a
lower bound ("at least" that many instances); leftover NodeClaims without a holding run count as instances but do
not add to the spend. It applies to forced runs; autoPlan uses the same check.

`maxUsdPerHour`, when set, is checked before a run starts against the node prices of the holding runs. A run in
Pending or Provisioning has no node and no price yet, so while one exists a new run is refused ("wait until ... has
a price"). A run that has a node but no `scarpenter.sparecores.com/price` annotation counts as unknown spend: the
controller logs a warning and refuses the new run (the operator must check SCarpenter). When SCarpenter marks a
NodeClaim `price-incomplete: "true"`, the run records `status.priceIncomplete: true`; such a price understates the
spend, so the cap is checked against too low a sum.

The check is not atomic: two concurrent `run` commands for different SKUs can both pass it and exceed the limit by one.
The planner of `serve` is single-threaded, so autoPlan does not add to this race.
