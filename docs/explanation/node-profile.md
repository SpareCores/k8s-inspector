# Node profile and comparability

## Bench NodeClasses and NodePools

Defined by the cluster platform, one set per vendor. k8s-inspector only selects them by name.

| Setting | Value | Why |
|---|---|---|
| `spec.kubelet.footprint` | `Lean` (decision K18) | small reservations and limited daemon memory; makes 1 GiB types usable and leaves more for benchmarks |
| `kubelet.maxPods` | 10 (the `Lean` default) | anchor, step, flannel, kube-proxy and the probe Pod fit |
| `kubelet.evictionHard` | `memory.available: 50Mi`, `nodefs.available: 1%`, `imagefs.available: 1%` | llm fills the disk to ~10 % free and membench/vLLM use most RAM: the kernel OOM killer acts on the task, eviction should not |
| Image garbage collection | kubelet defaults | replaces Docker's prune between task groups |
| Root disk | 128 GiB (256 GiB for GPU) (decision K21) | images, the llm model, the spool |
| Local instance storage | `ignore` (left raw) | the storage benchmark measures raw disks |
| Instance metadata | `Open` ([security](security.md#instance-metadata-imds)) | resource tracker cloud detection |
| Butane | mask `docker.socket`, `systemd-sysupdate*`, `systemd-oomd`, the SSH key fetch units; stop vendor agents after Ready | less background noise |
| Billing retention | off | a node is never reused |
| NodePools `bench-<vendor>` (`-lowmem` for ≤ 2 GiB, `-gpu`) | taint `frankenscube/bench=true:NoSchedule`, label `frankenscube/pool=bench`, `WhenEmpty`, `consolidateAfter: 0s`, `expireAfter: 48h`, `terminationGracePeriod: 1h`, limits below the vendor quota, node repair off | dedicated, short-lived, bounded |

Measured on an OCI node with default settings: the node's own daemons use about 630 MiB RSS at idle, the default
kubelet reservation holds back about 1.5 GiB of an 8 GiB node, and the node is about 98 % idle. With `Lean` an 8 GiB
type leaves about 7.0 GiB allocatable instead of 5.9 GiB.

1 GiB types need `Lean` (the default reservation alone exceeds the node). If that is still too little, a
host-network-only bench node without flannel and kube-proxy saves about 130 MiB more (decision K16).

## Benchmark Pod settings

| Docker era | k8s-inspector |
|---|---|
| `--privileged --network=host` | `privileged: true`, `hostNetwork: true`, `dnsPolicy: Default` |
| `seccomp=unconfined`, `nofile=1048576`, `memlock=-1` for DB tasks | `seccompProfile: Unconfined` for all; rlimits set by `sc-run` |
| `--pid=host` for storage | `hostPID: true` on the storage step only |
| no CPU limits | no CPU limits (no CFS quota, so `nproc` and cgroup-aware tools see the whole node) |
| memory cap 0.85 × available for geekbench and vLLM | `limits.memory` = 0.85 × Node allocatable for those two (decision K9); everything else under the `kubepods.slice` limit |
| `shm_size=4g` for vLLM | emptyDir `medium: Memory`, `sizeLimit: 4Gi` at `/dev/shm`, vLLM only |
| `nice -n -20` in commands and images | unchanged |

`storage` runs last on a node, because it may unmount and overwrite free disks. The spool is on the boot disk, which
the storage benchmark treats as protected (it holds `/`). Until the Flatcar-specific protected mounts land in the
storage image (last phase, [roadmap](../roadmap.md)), a storage failure is accepted and recorded like any other.

## Comparability with the VM era

| Aspect | Docker era (Ubuntu VM) | k8s-inspector | Effect |
|---|---|---|---|
| Runtime | dockerd → containerd → runc | kubelet → containerd 2 → runc | same runc path |
| cgroup | `system.slice/docker-<id>.scope` | `kubepods.slice/…/cri-containerd-<id>.scope`, `kubepods.slice` cpu weight ~76 | matters only under contention from daemons that use ~2 % CPU |
| Memory | no cap except vLLM/geekbench | `kubepods.slice` capped at allocatable | slightly less headroom on small types |
| Network | host | host | identical (the pod network over the tailnet is never used by benchmarks) |
| OS, kernel | Ubuntu 24.04, kernel 6.8, services stopped | Flatcar, kernel 6.12, kubelet, tailscaled, flannel, kube-proxy | the main source of differences |

The differences are measured, not assumed: before a vendor moves from the legacy pipeline, a few of its SKUs run the
core benchmarks on both pipelines and the medians are compared ([roadmap](../roadmap.md)). `meta.json` records
`node_os`, `kubelet` and `runtime: k8s-containerd`, so results stay attributable. Whether to rerun the whole fleet on
the new platform is decision K6.
