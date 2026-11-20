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
# PREFIX must sit under the bucket rule's k8s-inspector/ prefix
export BUCKET=<bucket> PREFIX=k8s-inspector/spikes/p0 AWS_PROFILE=<profile> REGION=<bucket region>
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
./presign.py presign --bucket $BUCKET --prefix $PREFIX --run-id $RUN_ID --region $REGION $TASKS \
  --file llm/synthetic.bin --large llm/synthetic.bin \
  --large llm/resource_tracker.jsonl --large membench/resource_tracker.jsonl \
  --expires 14400 --out urls.json         # S3-compatible store (OCI): add --endpoint-url https://...
kubectl apply -f namespace.yaml
kubectl -n inspector create secret generic p0-urls --from-file=urls.json=urls.json
```

Objects are keyed `<PREFIX>/<RUN_ID>/tasks/<task>/<file>`. Every task gets `stdout`, `stderr`, `version`,
`result.json`, `resource_tracker.jsonl` and `manifest.json`; `--file <task>/<name>` adds more, `--large <task>/<name>`
adds multipart URLs (16 MiB x 64 parts = 1 GiB by default; `--part-mib` is at least 5). `--region` is required: URLs are
signed for it. `step-llm.yaml` writes a synthetic 64 MiB `synthetic.bin` into its spool, so the multipart path runs
for sure (`--multipart-mib 5` in the llm and membench steps; `resource_tracker.jsonl` stays a plain PUT unless it
grows beyond 5 MiB).

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

The key is `<task>/<file name relative to the task's spool directory>`. A file with `parts` (and a `part_size` above 0) goes through UploadPart when it
is larger than `--multipart-mib` (32, the spike steps use 5) or has no `put`; otherwise it is PUT. `sc-run` re-reads the file before every
request, so a re-signed Secret takes effect within the kubelet's refresh time (about a minute).

## 3. Anchor and node

```sh
./step.sh anchor.yaml
kubectl -n inspector get pod p0-anchor -w          # Pending -> Running
NODE=$(kubectl -n inspector get pod p0-anchor -o jsonpath='{.spec.nodeName}')
kubectl -n inspector get pod p0-anchor -o jsonpath='{.status.conditions}'   # Ready means sc-run is installed
```

The bench pools allow 1-8 GiB only (no 16 GiB types), and no 1 GiB type exists on OCI: the sizes to compare are
`A1.Flex.1-2`, `E5.Flex.1-4`, `E5.Flex.2-8`.

## 4. Steps

```sh
./step.sh step-probes.yaml $NODE
kubectl -n inspector wait pod/p0-probes --for=jsonpath='{.status.phase}'=Succeeded --timeout=40m
./step.sh step-stressngfull.yaml $NODE            # then the same for step-membench.yaml, step-llm.yaml
# sc-run writes the task's output only to the spool files (no tee): read it from the uploaded objects, or
kubectl -n inspector logs p0-stressngfull          # sc-run's own messages (upload retries, warnings)
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

`./presign.py abort ... --task <t>` aborts multipart uploads that stay open (unused large-file slots) after `complete`.
Without `--task` it aborts every open upload of the run, also those of a step that is still running.

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
kubectl -n inspector exec p0-membench -- cat /proc/self/cgroup   # memory.max is not readable here: a privileged container sees the host root cgroup
# from the host (debug Pod with the host mounted): cat /sys/fs/cgroup/kubepods.slice/memory.max /sys/fs/cgroup/kubepods.slice/memory.events
```

Take the summary every minute during membench and llm (`while sleep 60; do ...; done`). The `kubepods.slice` limit is capacity
minus kube-reserved and system-reserved (50 MiB above allocatable): read `memory.max` and `memory.events` of
`/sys/fs/cgroup/kubepods.slice` from the host, not from inside a step container. Record: peak memory, whether the kernel OOM killer hit the task (`oom_kills` in
`result.json`) or the kubelet evicted the Pod (phase `Failed`, reason `Evicted`), and free disk (`nodefs.available`
in the summary) while llm runs (it does not come near 1 %: it downloads about 15 GB on an 8 GiB node).

`membench` itself peaks at about 130 MiB here, so it neither evicts nor OOMs: `step-memhog.yaml` (`tail /dev/zero` under
`sc-run`, add a `limits.memory` with `sed` for the limited variant) drives the memory-limit observations instead.

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

Measured on OCI: the Node is gone 79-91 s after T0, the NodeClaim 0-1 s after the Node.

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

# OOM: a 256 Mi memory limit on memhog (membench peaks at ~134 MiB and cannot OOM at 256 Mi)
sed 's/p0-memhog/p0-oom/; s/resources: {requests: {cpu: 10m, memory: 64Mi}}/resources: {requests: {cpu: 10m, memory: 64Mi}, limits: {memory: 256Mi}}/' \
  step-memhog.yaml | ./step.sh - $NODE
kubectl -n inspector get pod p0-oom -o jsonpath='{.status.containerStatuses[0].state.terminated}{" "}{.status.containerStatuses[0].lastState}{"\n"}'
```

Record: the Pod phase and reason, the container `reason` (`Completed`, `OOMKilled`, `Error`), the exit code and the
`message`. Expected: `sc-run` survives (it sets `oom_score_adj=-1000`, which the kernel's group kill skips; the other processes of the task are killed together, which is expected). The Pod is `Succeeded` and the container shows exit code 0 with reason
`OOMKilled` (not `Completed`); the message and `result.json` have `exit_code: 137`, `error_msg: OOMKilled` and `oom_kills` above 0.
If `sc-run` is killed too (no `result.json`, container reason `OOMKilled`), the controller falls back to the reason: check
`kubectl logs` for the `warning: oom_score_adj` line. `cat /sys/fs/cgroup/memory.oom.group` inside the container
stays `1` (the kubelet's default), which is expected.

### `sc-run` survives an OOM

A quick synthetic check before the membench run (privileged Pod with a 256 Mi limit, `stress-ng` image; `sc-run` must be
installed by the anchor):

```sh
kubectl -n inspector run p0-oomcheck --image=ghcr.io/sparecores/stress-ng:main --restart=Never --overrides='{"spec":{"nodeName":"'$NODE'","hostNetwork":true,"tolerations":[{"operator":"Exists"}],"containers":[{"name":"c","image":"ghcr.io/sparecores/stress-ng:main","securityContext":{"privileged":true},"resources":{"limits":{"memory":"256Mi"}},"terminationMessagePolicy":"File","command":["/k8si/bin/sc-run","exec","--task","oomcheck","--spool","/k8si/runs/oomcheck","--urls","/nonexistent","--upload-budget","5s","--timeout","2m","--","stress-ng","--vm","1","--vm-bytes","1G","--vm-keep","-t","60s"],"volumeMounts":[{"name":"k8si","mountPath":"/k8si"}]}],"volumes":[{"name":"k8si","hostPath":{"path":"/var/lib/k8s-inspector"}}]}}'
kubectl -n inspector get pod p0-oomcheck -o jsonpath='{.status.containerStatuses[0].state.terminated.message}{"\n"}'   # oom_kills >= 1; exit_code is 0 here because stress-ng handles its worker kills itself (137 with memhog)
```

The upload fails (no `/nonexistent`), which is fine: only the termination message and `/var/lib/k8s-inspector/runs/oomcheck/result.json` matter.
Delete the Pod afterwards. The memhog variant with a 256 Mi limit (above) is the real check.
Also check that the `DeadlineExceeded` Pod still uploaded: `sc-run` gets SIGTERM and has `--grace` (15 s from the signal, covering task stop, upload and manifest) within the Pod's
`terminationGracePeriodSeconds` (20 s); `result.json` and the termination message come first. If `manifest.json` is
missing, run `flush`.

### Mechanics to observe (no number, a yes or no)

| Mechanism | How |
|---|---|
| Growing logs re-PUT every 60 s | during membench, `aws s3 ls s3://$BUCKET/$PREFIX/$RUN_ID/tasks/membench/` twice: `stdout` grows |
| Multipart with part retry | llm's `synthetic.bin` (64 MiB, 4 parts of 16 MiB) must show `"method": "multipart"` with 4 parts in its `manifest.json`, and `complete` + `verify` must pass (check the assembled size, 67108864 bytes); block the endpoint briefly (a node firewall rule is outside this repo) or watch `kubectl logs` for `upload ...` retries |
| URL re-signing during a step | presign with `--expires 600`, start `step-llm.yaml`, then `./presign.py presign ... --region $REGION --resign urls.json --expires 14400 --out urls-new.json` (keeps the multipart files, part size and part count of `urls.json`), `kubectl -n inspector create secret generic p0-urls --from-file=urls.json=urls-new.json --dry-run=client -o yaml \| kubectl apply -f -`; uploads must continue after 10 minutes (403 errors in `kubectl logs` until the kubelet refreshes the mount) |
| `flush` | `step-flush.yaml` after a step whose `verify` failed |
