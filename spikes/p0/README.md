# P0 spike kit (OCI)

Hand-written manifests, a first `sc-run` and a presign helper, to observe the mechanics listed in the
[roadmap](../../docs/roadmap.md) P0 row on a real `bench-oci` node. Everything writes to object storage only. Nothing
here publishes to sc-inspector-data.

| File | Purpose |
|---|---|
| `presign.py` | stands in for the controller: multipart uploads, presigned URLs, `complete`, `abort`, `verify` |
| `namespace.yaml`, `anchor.yaml` | namespace and the anchor Pod (edit the instance type) |
| `step-probes.yaml`, `step-stressngfull.yaml`, `step-membench.yaml`, `step-llm.yaml` | the step Pods |
| `step-flush.yaml` | the `flush` step for one task |
| `step.sh` | fills `__RUN_ID__`, `__NODE_NAME__`, `__TASK__` and applies |

Variables used below:

```sh
export RUN_ID=p0                      # also the Secret and spool directory name
export BUCKET=<bucket> PREFIX=spikes/p0 AWS_PROFILE=<profile>
TASKS="--task dmidecode --task lshw --task lscpu --task lstopo --task virtualization --task stressngfull --task membench --task llm"
```

## 1. Build and push the sc-run image

CI does this on every push to `main` (`ghcr.io/sparecores/k8s-inspector-sc-run:main`, amd64 and arm64). By hand:

```sh
make test build                       # dist/sc-run-linux-{amd64,arm64}
docker buildx build --platform linux/amd64,linux/arm64 \
  -t ghcr.io/sparecores/k8s-inspector-sc-run:main --push .
```

## 2. Presign and create the Secret

`urls.json` contains bearer URLs: keep it private. URLs die with the signing credentials, so use credentials that live
at least as long as the longest step (llm: about 2 h), or re-sign during the step (section 5).

```sh
./presign.py presign --bucket $BUCKET --prefix $PREFIX --run-id $RUN_ID $TASKS \
  --large llm/resource_tracker.jsonl --large membench/resource_tracker.jsonl \
  --expires 14400 --out urls.json
kubectl apply -f namespace.yaml
kubectl -n inspector create secret generic p0-urls --from-file=urls.json=urls.json
```

Objects are keyed `<PREFIX>/<RUN_ID>/tasks/<task>/<file>`. Every task gets `stdout`, `stderr`, `version`,
`result.json`, `resource_tracker.jsonl` and `manifest.json`; `--file <task>/<name>` adds more, `--large <task>/<name>`
adds multipart URLs (16 MiB x 64 parts = 1 GiB by default).

### urls.json schema

```json
{"version": 1,
 "objects": {
   "membench/stdout": {"put": "<presigned PutObject URL>"},
   "membench/resource_tracker.jsonl": {
     "put": "<presigned PutObject URL>",
     "upload_id": "<multipart upload id>", "part_size": 16777216,
     "parts": ["<presigned UploadPart URL for part 1>", "<part 2>", "..."]}}}
```

The key is `<task>/<file name relative to the task's spool directory>`. A file with `parts` goes through UploadPart when it
is larger than `--multipart-mib` (32) or has no `put`; otherwise it is PUT. `sc-run` re-reads the file before every
request, so a re-signed Secret takes effect within the kubelet's refresh time (about a minute).

## 3. Anchor and node

```sh
./step.sh anchor.yaml
kubectl -n inspector get pod p0-anchor -w          # Pending -> Running
NODE=$(kubectl -n inspector get pod p0-anchor -o jsonpath='{.spec.nodeName}')
kubectl -n inspector get pod p0-anchor -o jsonpath='{.status.conditions}'   # Ready means sc-run is installed
```

## 4. Steps

```sh
./step.sh step-probes.yaml $NODE
kubectl -n inspector wait pod/p0-probes --for=jsonpath='{.status.phase}'=Succeeded --timeout=40m
./step.sh step-stressngfull.yaml $NODE            # then the same for step-membench.yaml, step-llm.yaml
kubectl -n inspector logs p0-stressngfull          # sc-run tees the task's output
```

A step Pod that ended stays until deleted, so its status can still be read. Check the result of each step:

```sh
kubectl -n inspector get pod p0-membench -o jsonpath='{.status.containerStatuses[0].state.terminated.message}'
./presign.py complete --bucket $BUCKET --prefix $PREFIX --run-id $RUN_ID --task membench
./presign.py verify   --bucket $BUCKET --prefix $PREFIX --run-id $RUN_ID --task membench
```

`complete` completes the multipart uploads listed in `manifest.json`; `verify` HEADs every object of the manifest and
writes `status.json` next to the outputs (the task's commit point). Both take the task list, so `$TASKS` works too.
If `verify` reports missing objects, run a `flush` step, then `complete` and `verify` again:

```sh
TASK=membench ./step.sh step-flush.yaml $NODE
```

`./presign.py abort ...` aborts multipart uploads that stay open (unused large-file slots) after `complete`.

## 5. Release

```sh
date -u +%FT%TZ; kubectl -n inspector delete pod p0-anchor       # T0 of the teardown measurement
kubectl -n inspector delete pod -l inspector.sparecores.com/run=$RUN_ID   # step Pods (no owner reference in the spike)
```

Delete `urls.json` and the Secret when done. Abort leftovers with `presign.py abort`.

## Measurements

Write the numbers into the docs (run model, node profile, K16). `NODE` is the anchor's node. Timestamps from the API
have 1 s resolution.

### Step overhead with a watch

Run the steps one after the other as in section 4, then read the Pod timestamps:

```sh
kubectl -n inspector get pods -o custom-columns=NAME:.metadata.name,CREATED:.metadata.creationTimestamp,STARTED:.status.containerStatuses[0].state.terminated.startedAt,FINISHED:.status.containerStatuses[0].state.terminated.finishedAt
```

Overhead of a step = its `STARTED` minus its `CREATED` (create to container running), and the gap between the previous
`FINISHED` and the next `CREATED` is how fast the watch (`kubectl wait`) and you applied it. Use `kubectl get pods -w -o wide` in a second terminal to see the phase changes live.
Expect about 3-5 s per step when the image is already pulled; a first pull adds the pull time (`kubectl get events`).

### Eviction and the `kubepods` limit under membench / llm

```sh
kubectl get node $NODE -o jsonpath='{.status.capacity.memory}{"  "}{.status.allocatable.memory}{"\n"}'
kubectl get --raw "/api/v1/nodes/$NODE/proxy/configz" | python3 -m json.tool | grep -iE 'evictionHard|kubeReserved|systemReserved|maxPods|footprint' -A4
kubectl get --raw "/api/v1/nodes/$NODE/proxy/stats/summary" > summary-$(date +%s).json   # node + pod memory, node fs/imagefs
kubectl get node $NODE -o jsonpath='{range .status.conditions[*]}{.type}={.status} {end}{"\n"}'   # MemoryPressure, DiskPressure
kubectl -n inspector get events --sort-by=.lastTimestamp
kubectl -n inspector get pod p0-membench -o jsonpath='{.status.phase}{" "}{.status.reason}{" "}{.status.message}{"\n"}'
kubectl -n inspector exec p0-membench -- sh -c 'cat /proc/self/cgroup; cat /sys/fs/cgroup/memory.max /sys/fs/cgroup/memory.events'
```

Take the summary every minute during membench and llm (`while sleep 60; do ...; done`). The `kubepods.slice` limit equals
the allocatable memory: confirm with a privileged `kubectl debug node/$NODE` where the admission policy allows it, or by
the peak of `resource_tracker.jsonl`. Record: peak memory, whether the kernel OOM killer hit the task (`oom_kills` in
`result.json`) or the kubelet evicted the Pod (phase `Failed`, reason `Evicted`), and free disk (`nodefs.available`
in the summary) while llm fills it to about 10 %.

### Hold idle timeout

After the last step, keep the anchor and do not touch it. The anchor ends 45 minutes after the last heartbeat, which is
the end of the last step (heartbeat granularity 10 s, poll 10 s):

```sh
kubectl -n inspector get pod p0-anchor -o jsonpath='{.status.containerStatuses[0].state.terminated.finishedAt}{"\n"}'
kubectl -n inspector get pod p0-steplast -o jsonpath='{.status.containerStatuses[0].state.terminated.finishedAt}{"\n"}'  # the last step
```

For a quick check, set `--idle-timeout` to `3m` in `anchor.yaml`. To keep the node during a pause between steps:
`kubectl -n inspector exec p0-anchor -- /sc-run heartbeat /k8si/runs/$RUN_ID`.

### Teardown time

```sh
date -u +%T; kubectl -n inspector delete pod p0-anchor            # T0
while kubectl get node $NODE >/dev/null 2>&1; do sleep 5; done; date -u +%T          # Node removed
CLAIM=$(kubectl get nodeclaim -l karpenter.sh/nodepool=bench-oci -o name | head -1)    # taken before T0
while kubectl get $CLAIM >/dev/null 2>&1; do sleep 5; done; date -u +%T              # NodeClaim gone
```

Measured earlier on OCI: about 1 minute to Node removal, 4-8 minutes to NodeClaim removal.

### `node_overhead`: Standard vs Lean on three sizes

Per node, right after it is Ready and before any step (needs one anchor per size and footprint; the footprint is set on
the SCNodeClass by the cluster platform):

```sh
kubectl get node $NODE -o jsonpath='{.status.capacity.memory}{" "}{.status.allocatable.memory}{"\n"}'
kubectl get --raw "/api/v1/nodes/$NODE/proxy/stats/summary" | python3 -c '
import json,sys; n=json.load(sys.stdin)["node"]
print("available", n["memory"]["availableBytes"], "working set", n["memory"]["workingSetBytes"])
for c in n.get("systemContainers", []): print(c["name"], c["memory"]["workingSetBytes"])'
kubectl -n inspector exec p0-anchor -- /sc-run ready /k8si/bin && echo installed
```

`node_overhead` = capacity minus allocatable (reservation) and the working set of `kubelet`, `runtime`, `pods` (daemons).
Repeat for `Standard` and `Lean` on three sizes (for example 1, 4 and 16 GiB) and put the table into
[node profile](../../docs/explanation/node-profile.md). Also decide K16 from the 1 GiB result: does `Lean` leave enough
for the benchmarks?

### Termination message, `OOMKilled`, `DeadlineExceeded` as seen by the API

Normal end, with the compact result written by `sc-run`:

```sh
kubectl -n inspector get pod p0-stressngfull -o jsonpath='{.status.containerStatuses[0].state.terminated}{"\n"}'
```

Run these last: they reuse the task names, so they overwrite those tasks' objects.

```sh
# DeadlineExceeded: a 60 s step deadline on a long task
sed 's/activeDeadlineSeconds: 4260/activeDeadlineSeconds: 60/; s/p0-membench/p0-deadline/' step-membench.yaml | ./step.sh - $NODE
kubectl -n inspector get pod p0-deadline -o jsonpath='{.status.phase}{" "}{.status.reason}{" "}{.status.containerStatuses[0].state.terminated}{"\n"}'

# OOM: a 256 Mi memory limit on membench
sed 's/p0-membench/p0-oom/; s/resources: {requests: {cpu: 10m, memory: 64Mi}}/resources: {requests: {cpu: 10m, memory: 64Mi}, limits: {memory: 256Mi}}/' \
  step-membench.yaml | ./step.sh - $NODE
kubectl -n inspector get pod p0-oom -o jsonpath='{.status.containerStatuses[0].state.terminated}{" "}{.status.containerStatuses[0].lastState}{"\n"}'
```

Record: the Pod phase and reason, the container `reason` (`Completed`, `OOMKilled`, `Error`), the exit code and the
`message`. If the kernel kills the task but not `sc-run`, the Pod shows `Completed` with `exit_code: 137` and
`error_msg: OOMKilled` only in the message and `result.json`. If it kills `sc-run` too, the API shows `OOMKilled`.
Also check that the `DeadlineExceeded` Pod still uploaded: `sc-run` gets SIGTERM and has `--grace` (45 s) and the Pod's
`terminationGracePeriodSeconds` (60 s) for the final flush; if `manifest.json` is missing, run `flush`.

### Mechanics to observe (no number, a yes or no)

| Mechanism | How |
|---|---|
| Growing logs re-PUT every 60 s | during membench, `aws s3 ls s3://$BUCKET/$PREFIX/$RUN_ID/tasks/membench/` twice: `stdout` grows |
| Multipart with part retry | llm or membench `resource_tracker.jsonl` goes through parts; block the endpoint briefly (a node firewall rule is outside this repo) or watch `kubectl logs` for `upload ...` retries |
| URL re-signing during a step | presign with `--expires 600`, start `step-llm.yaml`, then `./presign.py presign ... --resign urls.json --expires 14400 --out urls-new.json`, `kubectl -n inspector create secret generic p0-urls --from-file=urls.json=urls-new.json --dry-run=client -o yaml \| kubectl apply -f -`; uploads must continue after 10 minutes (403 errors in `kubectl logs` until the kubelet refreshes the mount) |
| `flush` | `step-flush.yaml` after a step whose `verify` failed |
