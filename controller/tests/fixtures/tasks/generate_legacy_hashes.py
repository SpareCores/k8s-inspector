"""Regenerate legacy_hashes.json from the legacy sc-inspector (read-only, not run by pytest).

    uv run --no-project --with-requirements <sc-inspector>/requirements.txt \
        python generate_legacy_hashes.py <sc-inspector>/inspector > legacy_hashes.json
"""

import inspect
import json
import sys

sys.path.insert(0, sys.argv[1])
import dbaas_tasks  # noqa: E402
import lib  # noqa: E402
import tasks  # noqa: E402

hashes = {}
for module in (tasks, dbaas_tasks):
    for name, task in inspect.getmembers(module):
        if isinstance(task, lib.Task):
            hashes[name] = lib.task_hash(task)
print(json.dumps({"hashes": hashes}, indent=1, sort_keys=True))
