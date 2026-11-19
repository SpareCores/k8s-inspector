# Kubernetes objects

All namespaced objects live in namespace `inspector`. API group of the custom resource: `inspector.sparecores.com`.

## Labels and annotations

| Key | On | Value |
|---|---|---|
| `inspector.sparecores.com/run` | BenchmarkRun, anchor, steps, Secrets, Jobs | run name |
| `inspector.sparecores.com/role` | Pods, Jobs | `anchor`, `step`, `flush`, `dbaas` |
| `inspector.sparecores.com/vendor` | BenchmarkRun, Pods | vendor id |
| `inspector.sparecores.com/attempt` | anchor, steps | attempt number |
| `inspector.sparecores.com/task` | steps | task name (`probe` for the probe step) |
| `karpenter.sh/do-not-disrupt: "true"` | anchor, controller Pod | blocks voluntary disruption |

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
  force: false              # skip Keeper and should_start
  capacity: SpotFirst       # SpotFirst | OnDemand (default from the vendor setting)
  holdAfter: 0s             # keep the node after the last step (debugging)
  database: null            # DBaaS runs: {vendor, id, engineVersion}
status:
  phase: Running            # Pending | Provisioning | Running | Releasing | Publishing | Done | Failed | Cancelled
  progress: "7/19"          # tasks finished / planned
  currentStep: geekbench
  attempt: 1
  capacityType: spot
  node: <node name>
  region: eu-frankfurt-1
  zone: <zone>
  pricePerHour: "0.0337"
  variant: 0
  tasks:
  - {name: geekbench, state: Running, attempt: 1, start: …, end: null, exitCode: null, published: false, commit: null}
  attempts:
  - {n: 1, capacityType: spot, nodeClaim: …, start: …, end: null, outcome: null}   # Completed | SpotInterrupted | NodeLost | ProvisioningFailed | Cancelled
  conditions: []
```

Printer columns (all `.spec`/`.status` paths): Vendor, SKU, Phase, Progress, Step, Capacity, Attempt, Node, Age.

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
  hostNetwork: true
  dnsPolicy: Default
  nodeSelector: {karpenter.sh/nodepool: bench-oci, node.kubernetes.io/instance-type: oci.VM.Standard.E5.Flex.2-16}
  affinity: {nodeAffinity: {requiredDuringSchedulingIgnoredDuringExecution: {nodeSelectorTerms: [{matchExpressions:
    [{key: karpenter.sh/capacity-type, operator: In, values: [spot, on-demand]}]}]}}}
  tolerations: [{key: frankenscube/bench, operator: Exists}]
  containers:
  - name: hold
    image: ghcr.io/sparecores/sc-run@sha256:…          # FROM scratch, a few MB
    args: [hold, --install, /k8si/bin, --heartbeat-dir, /k8si/runs/<run_id>, --idle-timeout, 45m]
    readinessProbe: {exec: {command: [/sc-run, ready, /k8si/bin]}}
    resources: {requests: {cpu: 1m, memory: 8Mi}}
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
  activeDeadlineSeconds: 5820            # pull 15m + version 1m + timeout 60m + upload 10m + slack
  terminationGracePeriodSeconds: 60      # room for sc-run's final flush on eviction
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
              --version-cmd, "geekbench.sh --version",
              --, /usr/local/bin/resource-tracker, --, nice, -n, "-20", geekbench.sh]
    env:                                  # plus TRACKER_* (output path in the spool, run id, job name, image digest)
    - {name: SENTINEL_API_TOKEN, valueFrom: {secretKeyRef: {name: inspector-secrets, key: sentinel}}}
    - {name: BENCHMARK_SECRETS_PASSPHRASE, valueFrom: {secretKeyRef: {name: inspector-secrets, key: geekbench}}}
    securityContext: {privileged: true, seccompProfile: {type: Unconfined}}
    resources: {requests: {cpu: 10m, memory: 64Mi}, limits: {memory: <0.85 × Node allocatable>}}
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

- The probe step is one Pod with one container per probe task, each wrapped by `sc-run`.
- The `flush` step uses the `sc-run` image with `args: [flush, --spool, …, --urls, …]` and the same mounts.
- Secret `<run name>-urls` (owner: the BenchmarkRun): `urls.json` with presigned PUT URLs per output key and
  `{uploadId, parts: [url…]}` per large file, re-signed hourly.

## Other objects

| Object | Purpose |
|---|---|
| CRD `benchmarkruns.inspector.sparecores.com` | shipped in this repository's deployment manifests |
| ConfigMap `k8s-inspector` | settings ([configuration](configuration.md)) |
| ConfigMap `k8s-inspector-spot` | per-SKU interruption counts ([spot](../explanation/spot.md)) |
| Secret `inspector-secrets` | benchmark secrets (External Secrets) |
| Jobs `dbaas-up-<run>`, `dbaas-down-<run>` | DBaaS provisioning ([DBaaS](../explanation/dbaas.md)) |
| PriorityClass `benchmark` | low value, `preemptionPolicy: Never` |
