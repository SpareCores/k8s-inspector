# Tasks

The task catalogue (`catalogue/tasks.yaml`) describes every benchmark. It is ported from sc-inspector's `tasks.py`
and `dbaas_tasks.py`; the task list, images, commands, timeouts and priorities are the same.

## Fields

| Field | Type | Meaning |
|---|---|---|
| `name` | string | task name, also the directory name in git and in object storage |
| `image` | string | the legacy image reference with tag (`ghcr.io/sparecores/benchmark:main`); **hashed** |
| `digest` | `sha256:…` | pinned multi-arch index digest; Pods use `<image>@<digest>`; **not hashed**; bumped by a weekly pull request (`controller/scripts/refresh_digests.py`; the refresh workflow dispatches CI on it) |
| `images`, `digests` | list, map | vLLM only: the image ladder in probe order and one digest per image (`image` is the three joined with `\|`) |
| `entrypoint` | list | the image's ENTRYPOINT restated (Kubernetes `command` replaces it) |
| `command` | string or null | the legacy command string, arguments of the image as in `docker run <image> <command>`; **hashed** |
| `version_command` | string | legacy value, same semantics as `command`; run before the task, 60 s cap → `meta.version`. `sc-run` runs it with `sh -c`, so the Pod gets `entrypoint` + `version_command` as one shell-quoted line; the legacy text is not a hash input |
| `timeout` | duration | task timeout (`sc-run`); the step deadline adds pull 15 m + version 1 m + upload 10 m + slack |
| `priority`, `parallel` | number, bool | ordering; `parallel: true` tasks form the probe step |
| `transforms` | list | `raw`, `fetch_geekbench_results`; the legacy source text of each transform is part of the hash |
| `parse` | string | `dmidecode`, `openssl`, `stressnglongrun`, `lstopo` (run by the publisher) |
| `privileges` | set | `host_pid` (storage), `db_rlimits` (pgbench) |
| `memory_cap` | float | fraction of Node allocatable as the memory limit (0.85 for geekbench, vllm) |
| `shm_gib` | int | `/dev/shm` size in GiB (4 for vllm) |
| `gpu` | bool | needs the GPU (no CPU fallback); a node without an NVIDIA GPU (accelerator labels) → `-3` |
| `precheck_command`, `precheck_regex` | strings | the legacy lshw check, kept for the record; the controller does not run it |
| `secrets` | list | keys of Secret `inspector-secrets` the step gets: `sentinel` → `SENTINEL_API_TOKEN`, `geekbench` → `BENCHMARK_SECRETS_PASSPHRASE`, `huggingface` → `HF_TOKEN` (always for tasks with `models_cache`) |
| `env` | map | fixed variables of the step |
| `collect` | list | extra files to upload (`/tmp/pg-server.log:postgres.log`) |
| `large_outputs` | list | files that get multipart upload slots from the start |
| `workload_proxy`, `durability` | strings | as in sc-inspector; **hashed** |
| `dbaas` | bool, default false | DBaaS task: runs only in a DBaaS run (named explicitly), never in the default selection |
| `revision` | int, default 0 | deliberate rerun; hashed only when > 0, so existing hashes stay equal |
| `spot` | bool, default true | false forces on-demand for runs containing the task (`stressnglongrun`) |
| `models_cache` | bool | mount the per-node model cache at `/models` (llm, vllm) |

Eligibility rules (memory, vCPU, GPU, storage, allowlists, credits) are not catalogue fields: they come from Keeper
(`task_block_reason_codes`), which applies sc-crawler's `inspector_rules`, as today.

## Task hash

`task_hash` is reproduced byte for byte: SHA-1 over the sorted keys of
`{command, transform_output, image, workload_proxy, durability}` that the task has, each key name followed by
`str(value)`; for `transform_output` the value is the concatenated legacy source text of the transforms. vLLM's
`image` is the three images joined with `|`. With `revision > 0`, `revision` is added as a sixth key.

A golden test recomputes every task's hash and compares it with the `task_hash` values in sc-inspector-data's
`schedule/` metas, so switching a vendor reruns nothing by itself.

## Reruns

| Want | How |
|---|---|
| rerun a task everywhere | bump its `revision` |
| rerun a task on one SKU | `k8s-inspector run <vendor> <sku> --tasks <task> --force`, or the legacy trick of prefixing `task_hash` with `RERUN_` in that SKU's `meta.json` |
| pick up a rebuilt image | nothing reruns; new runs use the new digest, recorded in `meta.image` |
