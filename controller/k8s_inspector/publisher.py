"""Publisher: render finished tasks into sc-inspector-data, commit, push, mark published. The only git writer.

Owner: publisher agent. Implements docs/explanation/outputs.md "Publishing to git", state.md "Disaster
recovery", output-contract.md "Commits". Git through the `git` CLI via `subprocess` (blobless, sparse,
shallow checkout in a working directory; deploy key from `env.git_key_file` via `GIT_SSH_COMMAND`).

One cycle: tasks with `state: Done` and `published: false` in all BenchmarkRuns -> `git fetch` +
`reset --hard origin/<branch>` -> for each task read `status.json` and the outputs
(`Storage.read_json` / `read_bytes`), `render.render_task` (+ `render_variant`, `render_timing`) -> one
commit `k8s-inspector: publish <n> results (<run ids>)` -> push; on non-fast-forward start again at the fetch
-> `crd.update_status` sets `published: true` and `commit: <sha>` on those tasks (and `status.variant`);
a run whose tasks are all published moves to phase Done. Idempotent: a crash between push and patch
re-renders the same content, finds no diff and patches.
"""

import threading
from dataclasses import dataclass
from pathlib import Path

from k8s_inspector.kube import Kube
from k8s_inspector.model import EnvConfig, Settings
from k8s_inspector.storage import Storage


@dataclass(frozen=True)
class PublishResult:
    tasks_published: int
    commit: str | None  # None when nothing changed
    run_ids: tuple[str, ...]


class Publisher:
    def __init__(self, kube: Kube, storage: Storage, env: EnvConfig, workdir: Path) -> None:
        raise NotImplementedError

    def publish_once(self) -> PublishResult:
        """One publish cycle as described in the module docstring."""
        raise NotImplementedError

    def rescan(self, since_days: int) -> PublishResult:
        """Disaster recovery (`publish --rescan --since 7d`): list recent date prefixes of the enabled
        vendors' SKUs in object storage (`Storage.list_status_keys`), publish every task with a
        `status.json` that git does not have (the stale-start guard skips what it has)."""
        raise NotImplementedError

    def delete_expired_runs(self, settings: Settings) -> int:
        """Delete BenchmarkRuns whose tasks are all published, `publish.keep_run_hours` after the last one.
        Returns how many."""
        raise NotImplementedError

    def run_forever(self, settings_provider, stop: threading.Event) -> None:
        """Every `publish.interval_minutes` (and when `trigger()` is called): `publish_once`,
        `delete_expired_runs`; exceptions are logged, never fatal."""
        raise NotImplementedError

    def trigger(self) -> None:
        """Wake `run_forever` for an on-demand cycle (publish CLI / run completion)."""
        raise NotImplementedError
