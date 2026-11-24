# Decisions

Status: **accepted** (decided by the maintainers), **open** (recommendation in bold, awaiting a decision) or postponed.

| # | Decision | Status | Choice / recommendation |
|---|---|---|---|
| K1 | Sequencing tasks on a node | accepted | anchor Pod + one Pod per step ([run model](explanation/run-model.md)) |
| K2 | Controller placement | accepted | long-running Deployment, 1 replica, `apps` pool; not a CronJob, not on control planes |
| K3 | Output path | accepted | `sc-run` spool + presigned PUT / UploadPart straight to the final keys; controller creates and completes multipart uploads; node released only after verification ([output pipeline](explanation/outputs.md)) |
| K4 | Controller's object storage credentials | accepted | cluster OIDC → IAM role, 12 h sessions; no static keys |
| K5 | Task identity | accepted | legacy-compatible hash, digests pinned outside it, per-task `revision` |
| K6 | Rerun the fleet on the new platform | open | **decide after the first vendor's A/B**; if yes, cheapest SKUs first via `revision` |
| K7 | Exit codes | accepted | keep -1/-3/-4/-5/256/>0, add -6 for timeouts, drop -2; -4 retried 3× then -5, DBaaS included |
| K8 | GPU fallback, vLLM ladder, nvbandwidth size | accepted | adaptive in the controller (ported) |
| K9 | Memory limits | accepted | 0.85 × Node allocatable for geekbench and vLLM only; no CPU limits; step Pods request no memory |
| K10 | Publish granularity | accepted | per task, every 5 minutes |
| K11 | DBaaS provisioning | accepted | Job running Pulumi (DB only, networks pre-created), state in `dbaas-state/` of the controller bucket, client node first, DB destroy and node release in parallel on the way out, run phase in `status.database`, finalizer, stack sweeper ([DBaaS](explanation/dbaas.md), [vendor contract](how-to/add-dbaas-vendor.md)) |
| K12 | Multi-instance runs | accepted | deferred; nothing in the single-node design blocks two anchors per run |
| K13 | First target vendor | accepted | OCI, with autoPlan selecting the SKUs (Keeper lists the OCI SKUs and its eligibility rules apply as for any vendor; Keeper unchanged); forced runs are for operators. Results pushed to sc-inspector-data `main` reach sc-crawler and the published data, so the first OCI SKUs are published only after their outputs are checked ([roadmap](roadmap.md)) |
| K14 | Anchor implementation | accepted | `sc-run hold`, 45 min idle timeout, installs `sc-run` for the steps |
| K15 | Unsupported GPU classes (GRID/fractional, AMD, pre-Pascal) | accepted | not benchmarked on k8s; legacy until it retires, then dropped |
| K16 | Host-network-only bench nodes (no flannel/kube-proxy) | accepted | not adopted: keep flannel and kube-proxy. They use ~97-98 MB RSS and ~40-55 MiB cgroup working set; `Lean` leaves 1606 MiB allocatable on the real 2 GB OCI node and no 1 GiB type exists ([node profile](explanation/node-profile.md)). Re-open only if a vendor offers real 1 GiB types |
| K17 | Instance metadata on bench nodes | accepted | `Open` on the bench NodeClass; later pass cloud facts as env and return to the default |
| K18 | Node footprint on the bench pool | accepted | `Lean` on the bench NodePool: a flat 370 MiB reservation, 153-1145 MiB more allocatable than `Standard` on 2-8 GB nodes ([node profile](explanation/node-profile.md)) |
| K19 | Caps | accepted | 2 in flight per vendor (GPU 1), `$/h` per vendor; no batching: due work starts as soon as the vendor has room |
| K20 | Spot | accepted | spot-first with per-run and per-SKU budgets and on-demand fallback ([spot](explanation/spot.md)); SpotFirst for OCI, AWS, GCP; OnDemand elsewhere; defaults 2 attempts / 60 min lost / quarantine after 2 in 14 d |
| K21 | Root disk | accepted | 128 GiB, 256 GiB for GPU |
| K22 | Nodes that outlive every teardown layer | accepted | alert "bench node running too long" in the monitoring phase; no automatic out-of-cluster force-deleter for now |
| K23 | Model cache | accepted | per-node hostPath only |
| K24 | `parse.yml` | accepted | publisher parses at publish time; `parse.yml` kept for legacy until P8 |
| K25 | Eligibility source | accepted | keep the Keeper HTTP call (fail closed) |
| K26 | Taint and pool label prefix | accepted | `frankenscube/bench`, `frankenscube/pool`, consistent with the existing pools |
| K27 | Run state and lease | accepted | `BenchmarkRun` custom resource (lease + status); git holds results only ([state model](explanation/state.md)) |
| K28 | Monitoring stack | accepted | VictoriaMetrics + kube-state-metrics + VictoriaLogs + Fluent Bit (not on bench) + vmalert + Alertmanager, in the cluster platform; notifications go to the `#alerts` channel of the Spare Cores Slack (Alertmanager → Slack) |
| K29 | Where results and code go | accepted | development on `main` of k8s-inspector; results to `main` of sc-inspector-data, no pilot branch |
| K30 | Legacy pipeline | accepted | stopped vendor by vendor once k8s-inspector is proven for it; never both for one vendor; no coexistence logic |
| K31 | Languages | accepted | controller in Python, latest stable CPython (3.15 at the time of writing; 3.14 if a dependency lacks wheels); `sc-run` in Go (one static binary for every image, standard library plus the embedded CA roots of `golang.org/x/crypto/x509roots/fallback`, a few MB of memory) |
| K32 | pgbench dataset upload to the CDN | accepted | drop it: the dataset is already on the CDN; runs only read it (`SC_CDN_UPLOAD=0`); no write access to the CDN prefix |
| K33 | Storage benchmark on Flatcar | accepted | fix sc-images last (P7); until then the storage task may fail on k8s nodes |
| K34 | OCI SKU names in sc-inspector-data | accepted | Keeper's `api_reference` (`<shape>.<ocpus>-<gb>` for flexible shapes), the names sc-crawler already uses |
| K35 | `apps` pool vendor | accepted (target) | UpCloud once SCarpenter's UpCloud worker support (being finished) has passed a real-cloud run; cluster-platform change |
| K36 | Web UI for runs | open (postponed) | later; `BenchmarkRun` printer columns serve `kubectl` meanwhile |
| K37 | Launch failures | accepted | SCarpenter's error class decides: temporary ones (`ICE`, `QUOTA`, `DAILY_LIMIT`, `VENDOR`) are retried with a backoff and leave nothing in sc-inspector-data, permanent ones (`LAUNCH_REJECTED`, `BOOT_FAILURE` the third time) are `-1` and re-checked after 30 days ([launch retries](explanation/launch-retries.md)) |
