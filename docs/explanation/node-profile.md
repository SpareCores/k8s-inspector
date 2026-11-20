# Node profile and comparability

## Bench NodeClass and NodePool

Defined by the cluster platform: one `bench` SCNodeClass with a block per vendor and one `bench` NodePool for all vendors
(the ConfigMap key `benchPool`). Adding a vendor adds its block to the SCNodeClass. There are no `-lowmem` or `-gpu`
pools: SCarpenter chooses GPU drivers per NodeClaim, and whether GPU types need their own pool for the 256 GiB root
disk is decided in P6. k8s-inspector only selects the pool by name.

| Setting | Value | Why |
|---|---|---|
| `spec.kubelet.footprint` | `Lean` (decision K18) | small reservations and limited daemon memory; makes 1 GiB types usable and leaves more for benchmarks |
| `kubelet.maxPods` | 10 (the `Lean` default) | anchor, step, flannel, kube-proxy and the probe Pod fit |
| `kubelet.evictionHard` | `memory.available: 50Mi`, `nodefs.available: 1%`, `imagefs.available: 1%` | a task that exhausts memory gets the kernel OOM killer, not eviction ([memory and disk behaviour](#memory-and-disk-behaviour)); llm downloads models bounded by RAM and stays far above 1 % free |
| Image garbage collection | kubelet defaults | replaces Docker's prune between task groups |
| Root disk | 128 GiB (256 GiB for GPU, see above) (decision K21) | images, the llm model, the spool |
| Local instance storage | `ignore` (left raw) | the storage benchmark measures raw disks |
| Instance metadata | `Open` ([security](security.md#instance-metadata-imds)) | resource tracker cloud detection |
| Butane | mask `docker.socket`, `systemd-sysupdate*`, `systemd-oomd`, the SSH key fetch units; stop vendor agents after Ready | less background noise |
| Billing retention | off | a node is never reused |
| NodePool `bench` | taint `frankenscube/bench=true:NoSchedule`, label `frankenscube/pool=bench`, `WhenEmpty`, `consolidateAfter: 0s`, `expireAfter: 48h`, `terminationGracePeriod: 1h`, no `limits` (every instance type of every vendor must be benchmarkable; the controller bounds running instances with `maxInFlight`), node repair off | dedicated, short-lived |

Without `Lean`, the default kube-reserved is min(255 MiB + 11 MiB × maxPods, 25 % of memory), at least 320 MiB.
`Lean` is a flat reservation: 192 MiB kube-reserved + 128 MiB system-reserved + 50 MiB eviction threshold = 370 MiB.

Measured on OCI eu-frankfurt-1 (Flatcar 4757.2.3, kernel 6.12.114, k3s v1.37.2, containerd 2.3.7), right after Ready,
with the anchor running:

| OCI VM | Footprint | Capacity | Allocatable | Reserved | maxPods | Node working set | `free` used |
|---|---|---|---|---|---|---|---|
| A1.Flex.1-2 (arm64, 2 GB) | `Lean` | 1976 MiB | **1606 MiB** | 370 MiB (18.7 %) | 10 | 287 MiB | 368 MiB |
| A1.Flex.1-2 (arm64, 2 GB) | `Standard` | 1976 MiB | 1453 MiB | 523 MiB (26.5 %) | 110 | 310 MiB | 394 MiB |
| E5.Flex.1-4 (amd64, 4 GB) | `Lean` | 3904 MiB | **3534 MiB** | 370 MiB (9.5 %) | 10 | 344 MiB | 558 MiB |
| E5.Flex.1-4 (amd64, 4 GB) | `Standard` | 3904 MiB | 2907 MiB | 997 MiB (25.5 %) | 110 | 366 MiB | 562 MiB |
| E5.Flex.2-8 (amd64, 8 GB) | `Lean` | 7930 MiB | **7560 MiB** | 370 MiB (4.7 %) | 10 | 351 MiB | 584 MiB |
| E5.Flex.2-8 (amd64, 8 GB) | `Standard` | 7930 MiB | 6415 MiB | 1515 MiB (19.1 %) | 110 | 366 MiB | 608 MiB |

Real OCI VMs are smaller than their nominal size (1976, 3904 and 7930 MiB), so allocatable is capacity − 370 MiB with
`Lean`. `Lean` gains 153, 627 and 1145 MiB over `Standard` on the three sizes. The daemons themselves hardly differ
between footprints (kubelet, containerd and tailscaled RSS 3-4 MB lower under `Lean`): the gain is the reservation. At
idle the node daemons (kubelet, containerd, tailscaled, flannel, kube-proxy, systemd) use about 450 MB RSS; the
kubelet cgroup working set is 38-48 MB.

No 1 GiB type exists in the OCI pool for either architecture (A1.Flex.1-1, E2.1.Micro match no instance type), so
1 GiB is not measured: a 1 GiB VM (about 970 MiB) minus 370 MiB leaves about 600 MiB allocatable, about 550 MiB
after flannel and kube-proxy ([K16](../decisions.md)). The bench pools allow 1-8 GiB; 16 GiB types are
unschedulable there.

## Memory and disk behaviour

Measured on the `Lean` E5.Flex.2-8 node.

- **Limit.** `kubepods.slice` `memory.max` is capacity − kube-reserved − system-reserved (7610 MiB on 7930 MiB): the
  eviction threshold is not subtracted, so the limit is 50 MiB above allocatable. `memory.max` is not readable from
  inside a privileged step container (it sees the host root cgroup); read it from the host.
- **OOM is global.** A runaway task (`tail /dev/zero`) filled the node in 5 s. The kernel ran out of memory before
  the `kubepods.slice` limit (system daemons hold about 440 MiB): `global_oom`, `memory.events` `max 0`, `oom_kill 3`.
  The kubelet did not evict: no Pod was evicted and `MemoryPressure` stayed `False`. `memory.oom.group` killed the
  task's whole process tree; `sc-run` (`oom_score_adj` -1000) survived and reported exit 137. The node got a
  `SystemOOM` event. With a 256 Mi container limit the result is the same. How the API shows it:
  [exit codes](../reference/exit-codes.md).
- **Benchmarks stay small.** membench peaks at 134 MiB pod working set, stressngfull and membench add 130-350 MiB over
  the idle node; llm adds up to about 2.3 GiB working set, mostly page cache of the mapped models.
- **DiskPressure at 1 %.** With 0.6 GB free (0.5 %) the kubelet met the threshold at once and image GC deleted every
  unused image (about 8 GiB); no Pod was evicted (anchor and step Pod kept running). The node got `DiskPressure=True`
  and the `node.kubernetes.io/disk-pressure:NoSchedule` taint, which stayed about 5 minutes after the space was free
  again (`evictionPressureTransitionPeriod`). New Pods are rejected meanwhile even with `spec.nodeName` (phase
  `Failed`, reason `Evicted`, message `Pod was rejected: The node had condition: [DiskPressure].`); the controller
  waits for the condition to clear ([run model](run-model.md#disk-pressure)). Image GC triggered by the eviction
  threshold deletes the cached benchmark images, so later steps pull their images again (llm image: 139 s).
- **llm disk use.** llm downloads only the models that fit the node's RAM: 5 models, 15 GB on the 8 GiB node. Free disk
  fell from 108 GB to 55 GB (45 % free) at the transient peak and returned to 87 GB (the models stay). It does not
  approach the 1 % threshold on that size.

## Benchmark Pod settings

| Docker era | k8s-inspector |
|---|---|
| `--privileged --network=host` | `privileged: true`, `hostNetwork: true`, `dnsPolicy: Default` |
| `seccomp=unconfined`, `nofile=1048576`, `memlock=-1` for DB tasks | `seccompProfile: Unconfined` for all; rlimits set by `sc-run` |
| `--pid=host` for storage | `hostPID: true` on the storage step only |
| no CPU limits | no CPU limits (no CFS quota, so `nproc` and cgroup-aware tools see the whole node); the step Pod's CPU **request** is what the other Pods leave ([CPU weight](#cpu-weight)) |
| memory cap 0.85 × available for geekbench and vLLM | `limits.memory` = 0.85 × Node allocatable for those two (decision K9); everything else under the `kubepods.slice` limit |
| `shm_size=4g` for vLLM | emptyDir `medium: Memory`, `sizeLimit: 4Gi` at `/dev/shm`, vLLM only |
| `nice -n -20` in commands and images | unchanged, and every task also runs at nice -20 via `sc-run exec --nice -20` (generic rule: benchmarks get as much CPU as possible; the legacy per-command `nice` stays, as nice clamps at -20) |

`storage` runs last on a node, because it may unmount and overwrite free disks. The spool is on the boot disk, which
the storage benchmark treats as protected (it holds `/`). Until the Flatcar-specific protected mounts land in the
storage image (last phase, [roadmap](../roadmap.md)), a storage failure is accepted and recorded like any other.

## CPU weight

Under cgroup v2, `nice` only orders threads inside one cgroup. Between cgroups the CPU share is the cgroup weight, and
the kubelet derives a Pod's weight from its CPU request. The weight of `kubepods.slice` against `system.slice` is fixed
by the kubelet. A step Pod is Burstable (it has a request but no limit), so it competes under
`kubepods.slice/kubepods-burstable.slice` with the other Burstable Pods. There the weights increase with the requests
(the kubelet converts requests to cgroup v1 shares, and shares to a v2 weight, a non-linear conversion), so a step Pod
requesting `cpu: 10m` would get the lowest weight, whatever its nice value.

The controller therefore sizes the request of each step Pod (not the flush Pod, which stays at 10m) at launch:

```
request = max(10m, node allocatable cpu − CPU requests of the other Pods on the node − headroom)
headroom = max(100m, the largest other Pod's request, 5% of the node allocatable cpu)
```

The other Pods are listed with the `spec.nodeName` field selector over all namespaces (anchor, flannel, kube-proxy,
debug Pods; finished Pods do not count; the controller's ClusterRole may `list` Pods for this). A Pod's request is
what the scheduler counts: container requests plus native sidecars, the larger init container peak, Pod overhead, or
the Pod-level request when set. Other Pods' requests are rounded up to whole millicores, ours down. The headroom
keeps the request below the allocatable total that the kubelet checks at admission, and leaves room for a replaced
or new DaemonSet Pod (as large as the largest Pod already there) to be scheduled without preempting a running
benchmark. The benchmark Pod then holds most of the weight inside `kubepods-burstable.slice`, and inside the Pod
`sc-run` runs the task at nice -20, so the task is first in line for CPU in every cgroup. A step with several
containers (the probe step) splits the request evenly (at least 1m each, 10m for the whole Pod). There is still no
CPU limit, so no CFS quota.

If a Pod appears on the node after the request was computed, the kubelet can refuse the step Pod
(`Failed`, reason `OutOfcpu`). The controller then lists the node's Pods again and creates the step Pod once more with
the recomputed request; a second refusal gives the 10m floor request; a third is recorded like any other rejection
(`-4` with the message). The number of refusals is kept in the annotation `inspector.sparecores.com/cpu-refusals` of
the recreated Pod, and every relaunch (also after a DiskPressure rejection) carries over the Pod's
`inspector.sparecores.com/*` annotations, so the relaunches are bounded. When the Pods of the node cannot be listed
(RBAC: Forbidden), the controller logs a warning and requests the floor; the launch is never blocked. Memory requests
stay at 64Mi.

## Comparability with the VM era

| Aspect | Docker era (Ubuntu VM) | k8s-inspector | Effect |
|---|---|---|---|
| Runtime | dockerd → containerd → runc | kubelet → containerd 2 → runc | same runc path |
| cgroup | `system.slice/docker-<id>.scope` | `kubepods.slice/…/cri-containerd-<id>.scope`, `kubepods.slice` cpu weight ~76 | `kubepods.slice` against `system.slice` is fixed by the kubelet and matters only under contention from daemons that use ~2 % CPU; inside `kubepods.slice` the step Pod has nearly all the weight ([CPU weight](#cpu-weight)) |
| Memory | no cap except vLLM/geekbench | `kubepods.slice` capped at allocatable | slightly less headroom on small types |
| Network | host | host | identical (the pod network over the tailnet is never used by benchmarks) |
| OS, kernel | Ubuntu 24.04, kernel 6.8, services stopped | Flatcar, kernel 6.12, kubelet, tailscaled, flannel, kube-proxy | the main source of differences |

The differences are measured, not assumed: before a vendor moves from the legacy pipeline, a few of its SKUs run the
core benchmarks on both pipelines and the medians are compared ([roadmap](../roadmap.md)). `meta.json` records
`node_os`, `kubelet` and `runtime: k8s-containerd`, so results stay attributable. Whether to rerun the whole fleet on
the new platform is decision K6.
