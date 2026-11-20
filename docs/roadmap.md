# Roadmap

Development happens on `main` of this repository. Results go straight to `main` of sc-inspector-data: the output is
compatible with sc-inspector's, so there is no pilot branch. A vendor is benchmarked by exactly one pipeline: the
legacy sc-inspector stops a vendor before k8s-inspector takes it over, so no coexistence logic exists.

Monitoring comes after the first results are published: manual triggering and `kubectl` are enough until then.

| Phase | Scope | Exit criteria |
|---|---|---|
| **P0 Spikes on OCI** | Hand-written YAML and a first `sc-run`, on the `bench` NodePool (`Lean` footprint, metadata `Open`): anchor + `nodeName` steps (probe Pod, stressngfull, membench, llm disk fill); spool + presigned PUT re-PUT, multipart with part retry, URL re-signing via the Secret during a step, `flush`; eviction behaviour and the `kubepods` limit under membench/llm; the `hold` idle timeout; teardown time; `node_overhead` with `Standard` vs `Lean` on three sizes; termination message, `OOMKilled`, `DeadlineExceeded` as seen by the API; step overhead with watch | Met: all mechanics observed; numbers written into these docs ([node profile](explanation/node-profile.md), [run model](explanation/run-model.md), [outputs](explanation/outputs.md)); K16 decided (not adopted); the spikes wrote to object storage only and published nothing to sc-inspector-data, because anything pushed to its `main` under `data/oci/...` flows into sc-crawler and the published data |
| **P1 MVP on OCI** | Controller (plan, run, publish) on the `apps` pool; `BenchmarkRun` CRD; forced runs only (Keeper's eligibility rules do not allow inspector runs on OCI SKUs, and Keeper stays unchanged); publishing OCI results to sc-inspector-data `main` per SKU, only after its outputs are checked to parse correctly and to match the legacy layout (sc-crawler already supports OCI and the published data includes it, so a published SKU goes live); OCI preemptible runs to exercise the spot strategy | 20 runs end to end; induced failures (pull error, OOM, timeout, node delete mid-step, controller restart mid-run, crash between push and status patch) classified and recovered; golden hash test green; no leaked node |
| **P2 Monitoring** | Cluster-platform stack (decision K28); controller `/metrics` and JSON logs; alerts incl. "bench node running too long" (K22) | Every alert fired once in a drill and reached a human |
| **P3 Vendor switch-over** | One vendor at a time ([add a vendor](how-to/add-vendor.md)): A/B against legacy on a few SKUs, legacy stops the vendor, `autoPlan` on. Order by SCarpenter lane status and credits; spot per vendor policy | Per vendor: 2 weeks without manual intervention, spend within caps |
| **P4 DBaaS** | Provisioner image and Jobs, pre-created networks; UpCloud first, then OVHcloud and Azure; legacy stops DBaaS per vendor | 0 leaked databases over 2 weeks; results comparable with legacy |
| **P5 1 GiB types** | 1 GiB types on the shared `bench` NodePool with `Lean`; Keeper memory thresholds revisited | runs complete on 2 SKUs per vendor |
| **P6 GPU** | Needs the planned SCarpenter GPU feature; AWS first | nvidia_smi, nvbandwidth, llm, vllm, ffmpeg within noise on 3 GPU SKUs |
| **P7 Storage benchmark on Flatcar** | sc-images `benchmark-storage`: Flatcar and Kubernetes paths in its protected mounts. Until then the storage task may fail on k8s nodes; the failure is recorded and the rest of the run is unaffected | storage results on 3 SKUs with instance storage |
| **P8 Retire legacy** | When every vendor is switched: archive sc-inspector's workflows and DBaaS path, `parse.yml` | nothing left on the legacy path |

## Parallel tracks outside this repository

| Track | Owner | Needed by |
|---|---|---|
| `apps` pool on UpCloud (hourly billing, most credits): needs SCarpenter's UpCloud worker support (being finished) to pass a real-cloud run, then a cluster-platform change; until then the controller runs on the current `apps` vendor | cluster platform | not blocking |
| One `bench` SCNodeClass (a block per vendor) and one `bench` NodePool, IAM role and bucket rules, External Secrets, namespace, admission policy | cluster platform | P0/P1 for OCI, P3 per vendor |
| Instance metadata access setting (per NodeClass) and its rollout | SCarpenter (in progress) | P1 |
| Spot interruption handling (notice detection, cordon/drain, reason on NodeClaim/Node) | SCarpenter (planned) | improves P1/P3, not required |
| GPU: driver sysext and NVIDIA runtime per NodeClaim | SCarpenter (planned) | P6 |
| Per-node tailnet keys | SCarpenter | before P3 |
| Web UI for runs (postponed, K36) | cluster platform | later |
