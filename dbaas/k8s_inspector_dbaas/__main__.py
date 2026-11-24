"""`python -m k8s_inspector_dbaas up|down|inventory`: the entry point of the Jobs `dbaas-up-<run>`, `dbaas-down-<run>`
and `dbaas-inventory-<vendor>`.

The Job has no Kubernetes API access and reports through the termination message (`/dev/termination-log`): `up` the
connection facts as JSON (a vendor-generated password in plain text: docs/explanation/dbaas.md), `inventory` the tagged
databases, any command `{"error": "..."}` on failure (the password is removed from the text). The exit code is 0 on
success. `down` needs only `DBAAS_RUN`, the BenchmarkRun name, and exits 0 once nothing is left; `inventory` needs
`DBAAS_VENDOR`.

When the Pod is stopped (SIGTERM) the Pulumi CLI gets SIGINT, which makes it finish the operation in flight and record
it in the state; the Pod has a long grace period for that.
"""

import contextlib
import json
import os
import signal
import sys
import tempfile
from pathlib import Path

from k8s_inspector_dbaas import stack, vendors
from k8s_inspector_dbaas.spec import load_spec

TERMINATION_LOG_ENV = "TERMINATION_LOG"
MAX_MESSAGE = 3000  # the kubelet keeps 4 KiB
INVENTORY_LISTED = 10  # databases named in the inventory message besides their number: the oldest


def _report(message: str) -> None:
    with open(os.environ.get(TERMINATION_LOG_ENV, "/dev/termination-log"), "w") as f:
        f.write(message[:MAX_MESSAGE])


def use_state_credentials() -> None:
    """`K8SI_STATE_AWS_CONFIG` is the text of an AWS config file with the profile of the state bucket (the backend URL
    names it): write it and point `AWS_CONFIG_FILE` at it. No AWS credential variable is set."""
    config = os.environ.get("K8SI_STATE_AWS_CONFIG")
    if config:
        fd, path = tempfile.mkstemp(prefix="aws-config-")
        with os.fdopen(fd, "w") as f:
            f.write(config)
        os.environ["AWS_CONFIG_FILE"] = path


def inventory_message(databases: list[dict]) -> str:
    """The number of tagged databases, the creation time of the oldest and the oldest ones, which fit the message."""
    oldest_first = sorted(databases, key=lambda d: d["created_at"])
    return json.dumps(
        {
            "count": len(oldest_first),
            "oldest_created_at": oldest_first[0]["created_at"] if oldest_first else None,
            "databases": oldest_first[:INVENTORY_LISTED],
        }
    )


def run(command: str) -> int:
    password = os.environ.get("DBAAS_PASSWORD", "")
    try:
        if command == "up":
            _report(stack.up(load_spec()).to_json())
        elif command == "down":
            stack.down(os.environ["DBAAS_RUN"])
        else:
            _report(inventory_message(vendors.load(os.environ["DBAAS_VENDOR"]).inventory()))
    except Exception as e:
        message = str(e).replace(password, "***") if password else str(e)
        print(f"{command} failed: {message}", file=sys.stderr)
        _report(json.dumps({"error": message[-2000:]}))
        return 1
    return 0


def _children() -> list[int]:
    """The direct child processes of this one (Linux): the Pulumi CLI."""
    found = []
    for path in Path(f"/proc/{os.getpid()}/task").glob("*/children"):
        found += [int(pid) for pid in path.read_text().split()]
    return found


def interrupt_children(signum: int, frame) -> None:
    """SIGTERM handler: pass it on to the Pulumi CLI as SIGINT, so it finishes and records the operation in flight (a
    SIGTERM would only make it stop). With nothing running there is nothing to record: exit."""
    children = _children()
    if not children:
        sys.exit(128 + signal.SIGTERM)
    for pid in children:
        with contextlib.suppress(ProcessLookupError):
            os.kill(pid, signal.SIGINT)


def main(argv: list[str]) -> int:
    if len(argv) != 2 or argv[1] not in ("up", "down", "inventory"):
        print("usage: python -m k8s_inspector_dbaas up|down|inventory", file=sys.stderr)
        return 2
    use_state_credentials()
    signal.signal(signal.SIGTERM, interrupt_children)
    return run(argv[1])


if __name__ == "__main__":
    sys.exit(main(sys.argv))
