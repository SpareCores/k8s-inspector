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
| Instance metadata | `Open`; on AWS also hop limit 2, `httpTokens: required` ([security](security.md#instance-metadata-imds)) | resource tracker cloud detection, also from a Pod's own network namespace (hop limit 2 on AWS) |
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
| `--privileged --network=host` | `privileged: true`, `hostNetwork: true`, `dnsPolicy: Default`; `redis` and `static_web` use the Pod's own network namespace instead ([loopback tasks](#loopback-tasks)) |
| `seccomp=unconfined`, `nofile=1048576`, `memlock=-1` for DB tasks | `seccompProfile: Unconfined` for all; rlimits set by `sc-run` |
| `--pid=host` for storage | `hostPID: true` on the storage step only |
| no CPU limits | no CPU limits (no CFS quota, so `nproc` and cgroup-aware tools see the whole node); the step Pod's CPU **request** is what the other Pods leave ([CPU weight](#cpu-weight)) |
| memory cap 0.85 × available for geekbench and vLLM | `limits.memory` = 0.85 × Node allocatable for those two (decision K9); everything else under the `kubepods.slice` limit |
| `shm_size=4g` for vLLM | emptyDir `medium: Memory`, `sizeLimit: 4Gi` at `/dev/shm`, vLLM only |
| `nice -n -20` in commands and images | unchanged, and every task also runs at nice -20 via `sc-run exec --nice -20` (generic rule: benchmarks get as much CPU as possible; the legacy per-command `nice` stays, as nice clamps at -20) |

`storage` runs last on a node, because it may unmount and overwrite free disks. The spool is on the boot disk, which
the storage benchmark treats as protected (it holds `/`). Until the Flatcar-specific protected mounts land in the
storage image (last phase, [roadmap](../roadmap.md)), a storage failure is accepted and recorded like any other.

## Loopback tasks

`redis` (memtier against local servers) and `static_web` (wrk against binserve) talk only over loopback. In the node's
network namespace every loopback packet passes the netfilter hooks that kube-proxy (`INPUT`/`OUTPUT` jumps to
`KUBE-FIREWALL`, `KUBE-SERVICES`, `KUBE-NODEPORTS`, conntrack matches), tailscale (`ts-input`) and flannel leave there.
A Pod's own namespace has none of them, so these two tasks set `host_network: false` in the catalogue
(`hostNetwork: false`, `dnsPolicy: Default`, outbound traffic still works through flannel, the benchmark traffic itself
stays on the Pod's `lo`). The step Pod is otherwise unchanged: `privileged`, `seccompProfile: Unconfined`, requests only.

Consequences of the Pod's own namespace for these tasks:

- The resource tracker reports the Pod's name and IP as the host identity (not the node's hostname and address) and sees
  only the Pod's `eth0` in its network metrics, not the node's interfaces.
- On AWS the metadata service is two hops away, so the bench NodeClass sets the IMDS hop limit to 2
  ([security](security.md#instance-metadata-imds)).

## Measurement conditions

Two settings shape how loopback- and syscall-heavy results compare with the VM era: the Pod network namespace of the
loopback tasks (above) and the kernel's CPU vulnerability mitigations.

- **Pod network namespace: used** for `redis` and `static_web`.
- **Kernel mitigations: on**, as on the legacy sc-inspector VMs. The node's mitigations match the VM's (same
  `lscpu` Vulnerability lines). Booting with `mitigations=off` would gain about 10-19 % on syscall- and loopback-heavy
  tasks, but then every task's results would diverge from legacy, so bench nodes do not use it.

Measured on one Azure `Standard_D2ds_v5` (Xeon 8370C, 2 vCPU), median of 3 runs each, same image digests and
commands as the catalogue. The legacy row is the Ubuntu VM result of the same SKU (sc-inspector-data commit
`ba01cd99cdd`). redis is SET requests per second at pipeline 1/4/16/512; static_web is 1 KiB requests per second with
2 threads at 2/8/32 connections.

| Variant | redis p1 | p4 | p16 | p512 | web 2c | 8c | 32c |
|---|---|---|---|---|---|---|---|
| legacy VM | 117537 | 333787 | 613729 | 840007 | 84206 | 105162 | 116874 |
| host network (before) | 93266 | 285357 | 559876 | 775870 | 77186 | 96257 | 106296 |
| **Pod network (chosen)** | 122206 | 332942 | 600650 | 779868 | 103235 | 120188 | 135849 |
| host network, `mitigations=off` | 111289 | 318063 | 592843 | 783261 | 89807 | 109814 | 120209 |
| Pod network, `mitigations=off` (not used) | 135528 | 357966 | 622644 | 796722 | 113852 | 132205 | 149332 |

Other findings, all on the host network unless noted:

- `RuntimeDefault` seccomp (unprivileged) is about 11 % worse on redis p1 (83142), which is why step Pods stay
  `privileged` with `Unconfined`.
- CPU requests, the `benchmark` priority class, cgroup placement (`system.slice`), the resource-tracker wrapper and
  `taskset` pinning change nothing outside the noise (+-1-4 %). A Guaranteed Pod (`limits.cpu` 1.8) loses 5 % at p512
  to CFS throttling (the reason for "no CPU limits"). `sched_autogroup` and transparent huge pages (`never`) change
  nothing measurable.
- Accepting loopback early in the node's `iptables` plus `notrack` for `lo` (netfilter in the host namespace) recovers
  only about a third of the gap (redis p1 102858), so the cost is not conntrack alone and a node-level rule is no
  substitute for the Pod namespace.
- redis p512 stays about 7 % under legacy in every variant (unattributed).
- Azure guests expose no cpufreq or cpuidle driver, so governor and idle polling cannot be set.
- Not tested: `cpuManagerPolicy: static` (SCarpenter's SCNodeClass has no field for it) and other vendors.

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
(RBAC: Forbidden), the controller logs a warning and requests the floor; the launch is never blocked.

### Memory request

Step Pods and flush Pods request no memory: every container has an explicit `requests.memory: "0"`. It is explicit
mainly because a container with `limits.memory` and no request gets a request equal to the limit (geekbench and vLLM
have a limit), and a LimitRange default request would be added to a missing one too. A fixed request would be
rejected at kubelet admission on a small node ("Pod was rejected: Node didn't have enough resource: memory", reason
`OutOfmemory`), and every task of the run would fail without running; the controller records such a rejection as
`-4` with the message, like `OutOfcpu` and DiskPressure, so the planner retries it after its wait. Step Pods are
bound to the node with `nodeName`, so the scheduler does not place them, and the CPU request keeps them Burstable.
The anchor keeps its 16Mi request. The memory limit of the two capped tasks (`memory_cap`, geekbench and vLLM) is
`memory_cap` × the node's allocatable memory; a limit above what the node can give is fine, the kubelet admits on
requests only. A task killed by the OOM killer ends with exit code 137 and `OOMKilled`; under node memory pressure the
kubelet can instead evict the Pod, which is recorded as `-4` with `EVICTED`.

Measured on Vultr nodes of 0.5, 1, 2 and 4 GiB, with one 16Mi-request Pod and one request-0 Pod eating memory: the
request-0 Pod was the one killed or evicted in every measurement, and the 16Mi Pod survived. Allocatable was 78, 581,
1587 and 3535 MiB (a 512 MB node has about 77 MiB, which its anchor and DaemonSets already largely request), and a
benchmark could use about 45, 600, 1600 and 3510 MiB. On the smallest nodes the kubelet's memory eviction
(`memory.available` counts active page cache) limits a benchmark before physical memory does. The numbers depend on
SCarpenter's reservations for the Lean small nodes and change with them.

## Comparability with the VM era

| Aspect | Docker era (Ubuntu VM) | k8s-inspector | Effect |
|---|---|---|---|
| Runtime | dockerd → containerd → runc | kubelet → containerd 2 → runc | same runc path |
| cgroup | `system.slice/docker-<id>.scope` | `kubepods.slice/…/cri-containerd-<id>.scope`, `kubepods.slice` cpu weight ~76 | `kubepods.slice` against `system.slice` is fixed by the kubelet and matters only under contention from daemons that use ~2 % CPU; inside `kubepods.slice` the step Pod has nearly all the weight ([CPU weight](#cpu-weight)) |
| Memory | no cap except vLLM/geekbench | `kubepods.slice` capped at allocatable | slightly less headroom on small types |
| Network | host | host, except the loopback-only tasks `redis` and `static_web`: the Pod's own network namespace | identical for host tasks; the two loopback tasks avoid the node's netfilter rules ([loopback tasks](#loopback-tasks)) |
| Kernel mitigations | on | on, identical to the VM's | same; `mitigations=off` is not used ([measurement conditions](#measurement-conditions)) |
| OS, kernel | Ubuntu 24.04, kernel 6.8, services stopped | Flatcar, kernel 6.12, kubelet, tailscaled, flannel, kube-proxy | the main source of differences |

The differences are measured, not assumed: before a vendor moves from the legacy pipeline, a few of its SKUs run the
core benchmarks on both pipelines and the medians are compared ([roadmap](../roadmap.md)). `meta.json` records
`node_os`, `kubelet` and `runtime: k8s-containerd`, so results stay attributable. Whether to rerun the whole fleet on
the new platform is decision K6.
