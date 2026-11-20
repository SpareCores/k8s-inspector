# Add a vendor

*Planned. A vendor moves from the legacy sc-inspector to k8s-inspector as a whole; the two never run the same
vendor.*

## Preconditions

- SCarpenter's lane for the vendor is Supported, or at least its real-cloud smoke run passed and the
  vendor's quirks are understood ([vendors](../reference/vendors.md)).
- The cluster platform has, for the vendor: bench SCNodeClass(es) and NodePools `bench-<vendor>` (and `-lowmem`,
  `-gpu` when needed) as in [node profile](../explanation/node-profile.md), the vendor quota raised as far as `maxInFlight` needs (the NodePools have no `limits`), and (for DBaaS)
  pre-created networks in the targeted regions.

## Steps

1. Add the vendor to the platform-managed ConfigMap `k8s-inspector` (key `config.yaml`; applies within ~30 s, no restart) with `enabled: true`, `autoPlan: false`, `maxInFlight: 1`.
2. Run 3-5 representative SKUs with forced runs ([start a forced run](run-forced.md)) and check the results in git.
3. Compare with the legacy results of the same SKUs: the core benchmarks' medians should be within run-to-run noise
   (about ±3 %) or the difference explained.
4. Stop the vendor in the legacy pipeline (remove it from its start workflow).
5. Set `autoPlan: true` and the final caps and spot policy.
6. Watch for two weeks: no manual intervention needed, spend within caps, no alerts left open.
