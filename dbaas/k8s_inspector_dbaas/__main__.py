"""`python -m k8s_inspector_dbaas up|down`: the entry point of the Jobs `dbaas-up-<run>` and `dbaas-down-<run>`.

The Job has no Kubernetes API access. `up` reports through the termination message (`/dev/termination-log`): the
connection facts as JSON on success, `{"error": "..."}` on failure (the password is removed from the text). The exit
code is 0 on success. `down` needs only `DBAAS_RUN`, the BenchmarkRun name, and exits 0 once nothing is left.
"""

import json
import os
import sys

from k8s_inspector_dbaas import stack
from k8s_inspector_dbaas.spec import load_spec

TERMINATION_LOG_ENV = "TERMINATION_LOG"
MAX_MESSAGE = 3000  # the kubelet keeps 4 KiB


def _report(message: str) -> None:
    with open(os.environ.get(TERMINATION_LOG_ENV, "/dev/termination-log"), "w") as f:
        f.write(message[:MAX_MESSAGE])


def run(command: str) -> int:
    password = os.environ.get("DBAAS_PASSWORD", "")
    try:
        if command == "up":
            _report(stack.up(load_spec()).to_json())
        else:
            stack.down(os.environ["DBAAS_RUN"])
    except Exception as e:
        message = str(e).replace(password, "***") if password else str(e)
        print(f"{command} failed: {message}", file=sys.stderr)
        if command == "up":
            _report(json.dumps({"error": message[-2000:]}))
        return 1
    return 0


def main(argv: list[str]) -> int:
    if len(argv) != 2 or argv[1] not in ("up", "down"):
        print("usage: python -m k8s_inspector_dbaas up|down", file=sys.stderr)
        return 2
    return run(argv[1])


if __name__ == "__main__":
    sys.exit(main(sys.argv))
