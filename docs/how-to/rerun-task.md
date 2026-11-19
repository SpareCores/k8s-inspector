# Rerun a task

*Planned: describes the CLI once it exists.*

| Scope | Do this |
|---|---|
| One task on one SKU, now | `k8s-inspector run <vendor> <sku> --tasks <task> --force` |
| One task on one SKU, at the next planning cycle | in sc-inspector-data, prefix `task_hash` with `RERUN_` in `data/<vendor>/<sku>/schedule/<task>/meta.json` (and the variant's `meta.json`), commit to `main` |
| One task on every SKU | increase the task's `revision` in `catalogue/tasks.yaml`, merge; the planner reruns it as SKUs come due |
| A task stuck at `-5` | either of the above: a hash change clears the retry cap |

A rebuilt image (new digest) reruns nothing; bump `revision` if the new image changes results.

Before a fleet-wide `revision` bump, check the cost: [vendors](../reference/vendors.md) and the per-vendor caps in
[configuration](../reference/configuration.md) bound how fast it proceeds.
