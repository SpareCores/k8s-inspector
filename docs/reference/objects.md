# Kubernetes objects

All namespaced objects live in namespace `inspector`. API group of the custom resource: `inspector.sparecores.com`.

## Labels and annotations

| Key | On | Value |
|---|---|---|
| `inspector.sparecores.com/run` | BenchmarkRun, anchor, steps, Secrets, Jobs | run name |
| `inspector.sparecores.com/role` | Pods, Jobs | `anchor`, `step`, `flush`, `dbaas` |
| `inspector.sparecores.com/dbaas` | DBaaS Jobs | `up` or `down` |
| `inspector.sparecores.com/vendor` | BenchmarkRun, Pods | vendor id |
| `inspector.sparecores.com/attempt` | anchor, steps | attempt number |
| `inspector.sparecores.com/task` | steps | task name (`probe` for the probe step) |
| `inspector.sparecores.com/cancel: "true"` | BenchmarkRun | cancel request (`k8s-inspector cancel`); the controller ends the run |
| `karpenter.sh/do-not-disrupt: "true"` | anchor | blocks voluntary disruption of the bench node |

## BenchmarkRun

`inspector.sparecores.com/v1alpha1`, namespaced, `status` subresource, no webhook, no conversion. Written only by the
controller (and created by the CLI).

```yaml
apiVersion: inspector.sparecores.com/v1alpha1
kind: BenchmarkRun
metadata:
  name: oci-vm-standard-e5-flex-2-16-3f9c1a7e     # <vendor>-<sku slug>-<crc32(vendor/sku)>: the lease
  namespace: inspector
  labels: {inspector.sparecores.com/vendor: oci}
spec:
  vendor: oci
  sku: VM.Standard.E5.Flex.2-16
  runId: r20261120t0930-3f9c
  tasks: []                 # empty = every due task; otherwise exactly these
  force: false              # skip Keeper's eligibility check and the vendor limits; launch failures end as -1 results
                            # (a forced DBaaS run also records a NO_OFFERING deferral for vendor+SKU: server runs of the SKU wait 24 h)
  capacity: SpotFirst       # SpotFirst | OnDemand | SpotUntilExhausted | SpotOnly (default from the vendor setting)
  holdAfter: 0s             # keep the node after the last step (debugging)
  regions: [eu-central-1]   # server runs: the regions autoPlan confined the run to (absent: any); every attempt's anchor requires one of them
  database: null            # DBaaS runs: {vendor, id, engineVersion, region, vcpus, memoryGib}
status:
  phase: Running            # Pending | Provisioning | Running | Releasing | Publishing | Done | Failed | Cancelled
  progress: "7/19"          # tasks finished / planned
  currentStep: 07-geekbench      # name of the step Pod created last
  step:                          # the step in flight, read back after a restart, never planned again
    {index: 7, name: 07-geekbench, gpuRuntime: false,
     containers: [{key: geekbench, image: …, command: …, timeoutSeconds: 3600, env: {…}}]}
  attempt: 1
  capacityType: spot             # of the node in use; absent while the attempt has no node
  node: <node name>
  region: eu-frankfurt-1
  zone: <zone>
  pricePerHour: "0.0337"
  priceIncomplete: false         # true: SCarpenter marked the price incomplete, so it understates the cost
  variant: 0
  tasks:
  - {name: geekbench, state: Running, attempt: 1, start: …, end: null, exitCode: null, published: false, commit: null}   # state: Pending | Running | Done; optional: errorMsg, gpuFallback
  attempts:
  - {n: 1, capacityType: spot, nodeClaim: …, node: …, start: …, end: null, outcome: null, launchEvents: [{name, time, tried, truncated, failed: [...], launched}]}   # Completed | SpotInterrupted | NodeLost | ProvisioningFailed | LaunchFailed | SpotUnavailable | SpotRejected | Cancelled | HarnessError
  spotReclaims: 0                # spot claims of the current attempt lost before they were Initialized
  firstSpotTry: …                 # spot modes: the SKU's first counted spot try, kept when the spot node cleared the tries
  uninitializedClaims: []       # NodeClaims of the current attempt seen launched and not Initialized: [{nodeClaim, launched, capacityType}]
  launchFailures: []             # NodeClaims of the current attempt that failed to launch: [{nodeClaim, message, time}], last 20
  conditions: []
```

Printer columns (all `.spec`/`.status` paths): Vendor, SKU, Phase, Database (`status.database.phase` of a DBaaS run), Progress, Step, Capacity, Attempt, Node, Age.

Lifecycle: created by the planner or the CLI; deleted by the controller 24 h after every task is published; deleting it
earlier cancels the run (owner references remove the anchor, steps and Secrets).

## Anchor Pod

```yaml
metadata:
  name: <run name>-a1
  ownerReferences: [{kind: BenchmarkRun, name: <run name>, controller: true}]
  annotations: {karpenter.sh/do-not-disrupt: "true"}
spec:
  restartPolicy: Never
  activeDeadlineSeconds: <Σ step deadlines + 1 h>    # counts from kubelet start
  priorityClassName: benchmark
  automountServiceAccountToken: false
  enableServiceLinks: false
  hostNetwork: true                      # false for tasks with `host_network: false` (redis, static_web), see node-profile.md#loopback-tasks
  dnsPolicy: Default
  nodeSelector: {karpenter.sh/nodepool: bench, scarpenter.sparecores.com/vendor: oci, inspector.sparecores.com/run: oci-vm-standard-e5-flex-2-16-3f9c1a7e, scarpenter.sparecores.com/api-reference: VM.Standard.E5.Flex.2-16}
  affinity: {nodeAffinity: {requiredDuringSchedulingIgnoredDuringExecution: {nodeSelectorTerms: [{matchExpressions:
    [{key: karpenter.sh/capacity-type, operator: In, values: [spot, on-demand]}]}]}}}
  tolerations: [{key: frankenscube/bench, operator: Exists}]
  containers:
  - name: hold
    image: ghcr.io/sparecores/sc-run@sha256:…          # FROM scratch, a few MB
    args: [hold, --install, /k8si/bin, --heartbeat-dir, /k8si/runs/<run_id>, --idle-timeout, 45m]
    readinessProbe: {exec: {command: [/sc-run, ready, /k8si/bin]}}
    resources: {requests: {cpu: 1m, memory: 16Mi}}
    volumeMounts: [{name: k8si, mountPath: /k8si}]
  volumes: [{name: k8si, hostPath: {path: /var/lib/k8s-inspector, type: DirectoryOrCreate}}]
```

## Step Pod (example: geekbench)

```yaml
metadata:
  name: <run name>-a1-07-geekbench
  ownerReferences: [{kind: Pod, name: <run name>-a1}]
spec:
  nodeName: <anchor's node>
  restartPolicy: Never
  activeDeadlineSeconds: 5460            # pull 15m + version 1m + longest timeout 60m + upload 10m + slack 5m
  terminationGracePeriodSeconds: 20      # sc-run's shutdown budget (--grace 15s) plus 5 s margin
  hostNetwork: true
  dnsPolicy: Default
  hostPID: false                         # true only for storage
  automountServiceAccountToken: false
  enableServiceLinks: false
  tolerations: [{operator: Exists}]
  # runtimeClassName: nvidia             # GPU types
  containers:
  - name: geekbench
    image: ghcr.io/sparecores/benchmark:main@sha256:…
    command: [/k8si/bin/sc-run, exec, --task, geekbench, --timeout, 60m,
              --spool, /k8si/runs/<run_id>/geekbench, --urls, /k8si-urls/urls.json,
              --multipart-mib, "32", --reput, 60s, --heartbeat-dir, /k8si/runs/<run_id>,
              --version-cmd, "/usr/local/bin/resource-tracker -- bash -c '…geekbench6 --version | awk …'",
              --, /usr/local/bin/resource-tracker, --, nice, -n, "-20", geekbench.sh]
    env:                                  # plus TRACKER_* (output path in the spool, run id, job name, image digest)
    - {name: SENTINEL_API_TOKEN, valueFrom: {secretKeyRef: {name: inspector-secrets, key: sentinel}}}
    - {name: BENCHMARK_SECRETS_PASSPHRASE, valueFrom: {secretKeyRef: {name: inspector-secrets, key: geekbench}}}
    securityContext: {privileged: true, seccompProfile: {type: Unconfined}}
    resources: {requests: {cpu: <allocatable − other Pods' requests − 10m, min 10m>, memory: "0"}, limits: {memory: <0.85 × Node allocatable>}}  # never a CPU limit
    terminationMessagePolicy: File
    volumeMounts:
    - {name: k8si, mountPath: /k8si}
    - {name: urls, mountPath: /k8si-urls, readOnly: true}
  volumes:
  - {name: k8si, hostPath: {path: /var/lib/k8s-inspector, type: Directory}}
  - {name: urls, secret: {secretName: <run name>-urls}}
  # llm/vllm: hostPath /var/lib/k8s-inspector/models → /models
  # vllm:     emptyDir {medium: Memory, sizeLimit: 4Gi} → /dev/shm
```

- How the API reports the end of a step Pod: normal end and OOM are `Succeeded` with the compact result in
  `containerStatuses[].state.terminated.message` (an OOM shows `exitCode: 0`, reason `OOMKilled`); a deadline is
  `Failed` with `status.reason: DeadlineExceeded` and the `sc-run` container `Completed`, exit code 0 and
  `exit_code: 143` in the message; a Pod refused at admission (`DiskPressure`: reason `Evicted`; `OutOfcpu`, `OutOfmemory`) is `Failed`
  with a `Pod was rejected: ...` message and no containers, recorded `-4` with the message ([exit codes](exit-codes.md#exit-codes)).
- Step deadline = image pull (`deadlines.imagePullMinutes`, 15 m) + version 1 m + the longest container timeout +
  upload 10 m + slack 5 m. The anchor deadline is the sum of the step deadlines (the probe step, every serial task,
  one more probe step per vLLM image) plus two flush Pods (15 m each) per step, `holdAfter` and 1 h.
- `--version-cmd` is `entrypoint` + the catalogue's `version_command`, shell-quoted: `sc-run` runs it with `sh -c`,
  while the catalogue value is, as `command`, arguments of the image (`--version`).
- The vLLM probe containers carry their own 18 min timeout (`--probe-only` waits for the server).
- The probe step is one Pod with one container per probe task, each wrapped by `sc-run`.
- The `flush` step uses the `sc-run` image with `args: [flush, --spool, …, --urls, …]` and the same mounts.
- Secret `<run name>-urls` (owner: the BenchmarkRun): `urls.json`, re-signed hourly ([schema](#url-file)).
- `sc-run` subcommands: `hold` (anchor), `exec` (a task), `flush`, and the helpers `install <dir>`, `ready <dir>`
  (exit 0 when the binary is installed) and `heartbeat <dir>`.

## URL file

`urls.json` in the run's Secret. `sc-run` re-reads it before every request, so a re-signed Secret takes effect when
the kubelet refreshes the mount.

```json
{"version": 1,
 "objects": {
   "<task>/stdout": {"put": "<presigned PutObject URL>"},
   "<task>/resource_tracker.jsonl": {
     "put": "<presigned PutObject URL>",
     "upload_id": "<multipart upload id>", "part_size": 16777216,
     "parts": ["<presigned UploadPart URL, part 1>", "<part 2>"]}}}
```

The key is the task name and the file name relative to the task's spool directory. A file with `parts` is uploaded
with UploadPart when it is larger than 32 MiB (or has no `put`), otherwise with PUT; re-signing keeps the
`upload_id`. Files without an entry stay in the spool and are listed as missing.

## `manifest.json`

Written by `sc-run` last, in the task's spool directory and its object prefix:

```json
{"version": 1, "task": "membench", "complete": true,
 "objects": {
   "stdout": {"method": "put", "size": 123, "md5": "…", "etag": "…"},
   "resource_tracker.jsonl": {"method": "multipart", "size": 40000000, "upload_id": "…", "part_size": 16777216,
                              "parts": [{"n": 1, "size": 16777216, "etag": "…"}]}},
 "missing": []}
```

The controller completes every `multipart` object from `parts`, `HEAD`-checks every `size`, and aborts the unused
multipart uploads. `sc-run` keeps what it has confirmed in `.uploaded.json` in the spool, which is how `flush` knows
what is left.

## Other objects

| Object | Purpose |
|---|---|
| CRD `benchmarkruns.inspector.sparecores.com` | shipped in this repository's deployment manifests |
| ConfigMap `k8s-inspector` | settings ([configuration](configuration.md)) |
| ConfigMap `k8s-inspector-spot` | per-SKU interruption counts ([spot](../explanation/spot.md)) |
| ConfigMap `k8s-inspector-launch-<vendor>` | launch failures per SKU, `skus` = JSON `{sku: {reason, since, count, next}}` ([launch retries](../explanation/launch-retries.md)) |
| Secret `inspector-secrets` | benchmark secrets (External Secrets) |
| Jobs `dbaas-up-<run>`, `dbaas-down-<run>` | DBaaS provisioning ([DBaaS objects](#dbaas-objects)) |
| PriorityClass `benchmark` | value `-10`, `preemptionPolicy: Never`, not the global default; shipped in `deploy/` |
| ResourceQuota `benchmark-pods` | `pods: 60` in `inspector`; shipped in `deploy/` |

## DBaaS objects

A DBaaS run ([DBaaS](../explanation/dbaas.md)) is a `BenchmarkRun` with `spec.database`
(`{vendor, id, engineVersion, region, clientRegion, client, vcpus, memoryGib, usdPerHour}`; `region` is the database's region and `clientRegion` the client node's, absent when it is the same; `client` is an operator's choice of client SKU, absent: any small x86_64 type, and `status.clientSku` the SKU of the node it got; `usdPerHour` is database plus the dearest matching client price, absent when Keeper does not know one); its `sku` is the database id (the run is named after it), its capacity always
`OnDemand`, and `metadata.finalizers` holds `inspector.sparecores.com/dbaas` until the database is destroyed. Its
`status.database` is the state of the managed database:

```yaml
status:
  database:
    phase: Ready            # Clearing | Creating | Bootstrapping | Ready | Destroying | Destroyed; absent: nothing was created
    since: …                # the phase began
    expires: …              # the database is a leak after this (also tagged on it)
    error: "DBAAS_CREATE_FAILED: …"   # why it could not be created or used; or the last destroy failure
    destroyFailures: 0      # failed destroy Jobs so far
    retryAfter: null        # the next destroy Job is created at this time
    timing:                 # published as timing files (output-contract.md); each key is set when it happens
      api_start: …          # the create Job is about to start
      api_end: …            # the create Job succeeded (the bootstrap starts)
      api_destroy_start: …  # the destroy Job is about to start
      api_destroy_end: …    # the destroy Job succeeded
```

| Object | Purpose |
|---|---|
| Job `dbaas-up-<run>` | creates the database (`backoffLimit: 0`, `activeDeadlineSeconds` = `dbaas.createMinutes`, never deleted while it runs; a failed one is kept for its logs); owner: the run; Pods labelled `inspector.sparecores.com/dbaas-job: <job>`, not `…/run`, so the run's own Pod handling never touches them. Both Jobs' Pods have `karpenter.sh/do-not-disrupt: "true"` and a 15 minute termination grace period. The names of the Jobs are `dbaas-up-`/`dbaas-down-` and the run name, shortened with a hash of it when that passes 63 characters; the run name is in the annotation `inspector.sparecores.com/dbaas-run` |
| Job `dbaas-down-<run>` | destroys the stack and removes its state (deadline 2 h per attempt, created again until it succeeds); labelled with the run, not owned by it (the run's foreground deletion must not wait for it; the controller deletes it after the success; a failed one stays until its retry replaces it, so `kubectl logs` works, and every Job is removed 24 h after it finishes), and for a leaked stack labelled `inspector.sparecores.com/dbaas-sweep` and deleted by the sweeper |
| Job `dbaas-inventory-<vendor>` | the sweeper's, every 30 minutes per configured vendor: lists the vendor's databases that carry the `sc_inspector_run` tag (deadline 15 min, no state bucket) |
| Secret `<run>-db` (owner: the run) | the run's own password as `K8SI_RUN_PASSWORD` (never overwritten; the Jobs get it as `DBAAS_PASSWORD`), then `SC_DB_PASSWORD` (the same, or the vendor-generated one once the up Job reported it), `SC_DB_HOST`, `SC_DB_PORT`, `SC_DB_USER`, `SC_DB_PASSWORD`, `SC_DB_NAME`, `SC_DB_SSLMODE`, `PGSSLMODE`, `SC_DB_BOOTSTRAP_USER`, `SC_DB_BOOTSTRAP_DATABASE`, `SC_DB_GRANT_ROLE`; read by the Job, the bootstrap Pod and the pgbench step (`envFrom`) |
| ConfigMap `<run>-boot` (owner: the run) | the text of the bootstrap script |
| Pod `<run>-a<n>-dbaas-boot` (role `dbaas`, owner: the anchor) | the bootstrap: `nodeName` of the client node, host network, the pgbench image, `python3 /dbaas/bootstrap.py`, `envFrom` the Secret |
| Secret `k8s-inspector-dbaas-state` | platform: the passphrase of the Pulumi stacks |
| Secret `k8s-inspector-dbaas-<vendor>` | platform, optional: the vendor's cloud credentials as environment of its Jobs |
| ServiceAccount `k8s-inspector-dbaas` | shipped in `deploy/`: the Jobs' identity (no Role); a vendor may name its own in `dbaas.vendors.<vendor>.serviceAccount` |
