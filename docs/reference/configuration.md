# Configuration

## Environment (Deployment)

Values are deployment configuration of the cluster platform, not part of this repository.

| Variable | Meaning |
|---|---|
| `K8SI_BUCKET`, `K8SI_BUCKET_REGION` | object storage bucket for outputs |
| `K8SI_ROLE_ARN` | IAM role assumed with the ServiceAccount token (web identity) |
| `K8SI_GIT_URL`, `K8SI_GIT_BRANCH` | sc-inspector-data remote and branch (`main`) |
| `K8SI_GIT_KEY_FILE` | mounted deploy key |
| `KEEPER_API_BASE`, `KEEPER_API_KEY_FILE` | Keeper API (eligibility) |
| `SENTRY_DSN` | error reporting |
| `K8SI_NAMESPACE` | namespace of runs (default `inspector`) |
| `K8SI_SC_RUN_IMAGE` | the `sc-run` image of anchors and flush steps, digest-pinned (`ghcr.io/sparecores/k8s-inspector-sc-run@sha256:…`); `serve` and `run` refuse to start without it |
| `K8SI_S3_ENDPOINT_URL` | optional S3-compatible endpoint, for object storage that is not AWS S3 (OCI) |

The controller serves Prometheus metrics on port 9090 (`/metrics`, [monitoring](../explanation/monitoring.md)).

## ConfigMap `k8s-inspector`

Read on every planning cycle; changes apply without a restart. The YAML is the value of key `config.yaml` of the
ConfigMap. An unknown key is logged as a warning and ignored; an invalid value is an error.

```yaml
planner:
  intervalMinutes: 5
  autoPlan: false                # true: plan from Keeper and git; false: only forced runs
vendors:
  oci:
    enabled: true
    autoPlan: false              # per-vendor override
    maxInFlight: 2               # 0 pauses the vendor
    maxUsdPerHour: 2.0
    capacity: SpotFirst          # SpotFirst | OnDemand (the default)
    publish: false               # true: the publisher pushes this vendor's results to sc-inspector-data
    nodePools: {cpu: bench-oci, lowmem: bench-oci-lowmem, gpu: null}
defaults:
  maxInFlight: 2
  maxInFlightGpu: 1
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
  orphanNodeClaimMinutes: 30
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
`publish: true` (default `false`: runs and outputs happen, git stays untouched). A vendor is enabled here when it is
removed from the legacy pipeline ([roadmap](../roadmap.md)); there is no shared routing file.

## CLI

`k8s-inspector serve | plan [--dry-run] | run <vendor> <sku> [--tasks …] [--force] [--on-demand] [--hold 2h] |
status [--failing] | cancel <run> | publish [--rescan --since 7d]`. Inside the cluster:
`kubectl -n inspector exec deploy/k8s-inspector -- k8s-inspector …`.
