# Module interfaces (temporary, for the implementing agents)

Rules: code against `model.py` types and the signatures in the modules; do not change a signature without
telling the owner of the other side (put the need in your report). Each module's docstring says what it
implements; the docs under `../docs/` are the spec. Fill in bodies, add tests under `tests/`, keep
`uv run ruff check . && uv run ruff format --check . && uv run pytest` green. Simplicity first: plain
functions and dataclasses, no frameworks. Rust rule from CLAUDE.md does not apply (Python).

Threads, not asyncio (see package docstring). Kubernetes I/O only through `kube.Kube` (plain dicts).

| Module | Owner | Public API | Implements |
|---|---|---|---|
| `model.py` | scaffold (done) | dataclasses, enums, `Config` | objects.md, configuration.md, exit-codes.md |
| `config.py` | config | `load_env`, `parse_settings`, `load_settings`, `load_config` | configuration.md |
| `log.py` | scaffold (done) | `setup_logging`, `JsonFormatter` | monitoring.md (logs) |
| `kube.py` | kube/crd | `Kube` (get/list/create/delete/merge_patch/apply_secret/watch/exec_heartbeat), `NotFound`, `AlreadyExists`, `Conflict`, `WatchEvent` | state.md (watch + resync), security.md RBAC |
| `crd.py` | kube/crd | `run_name`, `new_run_id`, `run_key`, `to_manifest`, `from_dict`, `status_to_dict`, `create_run`, `get_run`, `list_runs`, `update_status`, `delete_run`, `RunExists` | objects.md BenchmarkRun, state.md lease |
| `tasks.py` | tasks | `load_catalogue`, `all_tasks`, `get_task`, `task_hash`, `image_ref`, `select_tasks`, `precheck`, `allows_spot`, `TRANSFORM_SOURCES`; data in `catalogue/tasks.yaml` | tasks.md |
| `steps.py` | steps | `probe_step`, `next_step`, `retry_without_gpu` | run-model.md Steps, gpu.md |
| `storage.py` | storage | key layout fns, `Storage` (`plan_uploads`, `presign`, `plans_from_urls_json`, `read_manifest`, `read_json`, `read_bytes`, `finish_task`, `salvage`, `abort_open`, `write_status_json`, `write_run_json`, `list_status_keys`), `render_urls_json` | outputs.md, output-contract.md |
| `pods.py` | pods | names/labels, `render_anchor`, `render_step`, `render_flush_step`, `render_urls_secret`, deadlines, `classify`, Pod readers | objects.md, node-profile.md, security.md, exit-codes.md |
| `spot.py` | spot | `capacity_for_next_attempt`, `classify_node_loss`, `is_quarantined`, `record_interruption` | spot.md |
| `runner.py` | runner | `Runner` (`enqueue`, `reconcile`, `cancel`, `run_forever`) | run-model.md, state.md |
| `planner.py` | planner | `force_run`, `plan_cycle` (stub), `run_forever`, `PlannedRun` | run-forced.md, configuration.md |
| `keeper.py` | planner | `block_reasons` | decision K25 |
| `variant.py` | publisher | `parse_lscpu_stdout`, `variant_id_from_fields`, `build_variant_json`, `match_variant`, `pick_variant` | output-contract.md variants |
| `render.py` | publisher | `RenderInput`, `meta_from_status`, `render_task`, `render_variant`, `render_timing` | outputs.md Publishing step 3, output-contract.md |
| `publisher.py` | publisher | `Publisher` (`publish_once`, `rescan`, `delete_expired_runs`, `run_forever`, `trigger`) | outputs.md Publishing, state.md DR |
| `cli.py` | cli | `build_parser` (done), `main`, `cmd_*` | configuration.md CLI |

## Who calls whom

- `runner` -> `crd.update_status`, `crd.get_run/list_runs`, `tasks.select_tasks/precheck/allows_spot`,
  `steps.probe_step/next_step/retry_without_gpu`, `spot.*`, `storage.plan_uploads/presign/render_urls_json/
  plans_from_urls_json/finish_task/salvage/write_status_json/write_run_json`, `pods.render_*/classify/
  pull_stuck/...`, `kube.*` (create Pods/Secret, delete anchor, read Node and NodeClaim).
- `planner` -> `crd.create_run`, `tasks.select_tasks`, `keeper.block_reasons`.
- `publisher` -> `crd.list_runs/update_status`, `storage.read_json/read_bytes/list_status_keys`, `render.*`,
  `variant.pick_variant`, git CLI.
- `pods` and `steps` and `tasks` are pure; `model` imports nothing.

## Data flow

1. `planner.force_run` creates the BenchmarkRun (spec.tasks, capacity). 2. `runner` creates the anchor, then per
step: `steps.*` picks a `Step` of `StepContainer`s -> `storage.plan_uploads` -> `presign` -> `render_urls_json` ->
`kube.apply_secret(pods.render_urls_secret)` -> `pods.render_step` -> Pod. 3. On Pod end: `pods.classify` ->
`{key: TaskOutcome}` -> `storage.finish_task` -> `storage.write_status_json` (the commit point) ->
`crd.update_status` (`TaskStatus` Done). 4. `publisher` reads `status.json` + outputs, renders, pushes, sets
`published`. `status.json` content = the final meta.json fields (`render.meta_from_status` documents which keys) plus
Kubernetes facts (pod, reason, image digest, node, run_id, zone, capacity_type, price, spot_attempts, node_os,
kubelet, runtime, nodeclass_hash): the runner writes it, the publisher reads it. The runner agent and the
publisher agent must agree on exactly those keys; `render.meta_from_status` is the reading side, the runner
writes the keys named in its docstring and in `render.meta_from_status`.

## Decisions the docs left open (scaffold choices)

- Catalogue is YAML at `k8s_inspector/catalogue/tasks.yaml` (docs: `catalogue/tasks.yaml`), loaded by `tasks.py`.
- New env vars: `K8SI_SC_RUN_IMAGE` (the anchor/flush image) and optional `K8SI_S3_ENDPOINT_URL`.
- `classify` returns a dict per container key (the probe step has several containers).
- Multipart-capable standard files: `stdout`, `stderr`, `resource_tracker.jsonl` + `TaskDef.large_outputs`.
- Step deadline slack 5 min (spike) instead of objects.md's 11 min arithmetic.
- Variant number is assigned by the publisher at publish time; `status.variant` is written then.
- ConfigMap `k8s-inspector` holds the YAML under key `config.yaml`.
- `revision` joins the hash key set before sorting.
- `timing` is not a task (publisher output); `gpu_facts` is a normal probe task of the catalogue.
