# Agent rules

- **Don't change a task hash unless a rerun is wanted.** A changed `tasks.task_hash` reruns that benchmark on every
  instance type. Its inputs are `command`, `image`, `workload_proxy`, `durability`, `revision` and the transform
  source text (`TRANSFORM_SOURCES`, also sc-inspector's `inspector/transform.py`), so even a comment or a timeout
  added there counts. Keep infra, supporting-code and refactor changes out of them; for a deliberate rerun bump
  `revision`, and ask the user first.
