"""Command line: `k8s-inspector serve | plan | run | status | cancel | publish` (configuration.md "CLI").

Owner: cli agent. The parser is complete; the `cmd_*` functions wire the other modules together and are
stubs. All commands except `serve` run one-shot against the cluster (inside it via
`kubectl exec deploy/k8s-inspector -- k8s-inspector ...`).

`serve` wiring: `log.setup_logging`, `config.load_config`, `Kube`, `Storage`, `Runner`, `Publisher`; start
threads `runner.run_forever`, `planner.run_forever`, `publisher.run_forever`, an optional health/metrics
endpoint later; stop all on SIGTERM via one `threading.Event`.
"""

import argparse
import re
from datetime import timedelta

_DURATION = re.compile(r"^(\d+)([smhd])$")
_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400}


def parse_duration(text: str) -> timedelta:
    """`"2h"`, `"90m"`, `"7d"`, `"0s"` -> timedelta. Raise `argparse.ArgumentTypeError` otherwise."""
    m = _DURATION.match(text)
    if not m:
        raise argparse.ArgumentTypeError(f"invalid duration {text!r} (use e.g. 90s, 30m, 2h, 7d)")
    return timedelta(seconds=int(m.group(1)) * _UNITS[m.group(2)])


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="k8s-inspector", description="Spare Cores benchmark controller")
    sub = p.add_subparsers(dest="command", required=True)

    sub.add_parser("serve", help="run the controller (planner, runner, publisher)")

    plan = sub.add_parser("plan", help="run one planning cycle")
    plan.add_argument("--dry-run", action="store_true", help="print what would be started")

    run = sub.add_parser("run", help="start a forced run of one SKU")
    run.add_argument("vendor")
    run.add_argument("sku")
    run.add_argument("--tasks", type=lambda s: s.split(","), default=[], help="comma-separated task names")
    run.add_argument("--force", action="store_true", help="skip Keeper and the due check")
    run.add_argument("--on-demand", action="store_true", help="do not use spot")
    run.add_argument("--hold", type=parse_duration, default=timedelta(0), help="keep the node after the last step")

    status = sub.add_parser("status", help="list runs")
    status.add_argument("--failing", action="store_true", help="only failing tasks")

    cancel = sub.add_parser("cancel", help="cancel a run")
    cancel.add_argument("run")

    pub = sub.add_parser("publish", help="publish finished tasks now")
    pub.add_argument("--rescan", action="store_true", help="recover from object storage after a cluster loss")
    pub.add_argument("--since", type=parse_duration, default=timedelta(days=7))
    return p


def cmd_serve(args: argparse.Namespace) -> int:
    raise NotImplementedError


def cmd_plan(args: argparse.Namespace) -> int:
    raise NotImplementedError


def cmd_run(args: argparse.Namespace) -> int:
    """`planner.force_run`; print the run name. Exit 1 on `RunExists` / `ValueError`."""
    raise NotImplementedError


def cmd_status(args: argparse.Namespace) -> int:
    raise NotImplementedError


def cmd_cancel(args: argparse.Namespace) -> int:
    raise NotImplementedError


def cmd_publish(args: argparse.Namespace) -> int:
    raise NotImplementedError


COMMANDS = {
    "serve": cmd_serve,
    "plan": cmd_plan,
    "run": cmd_run,
    "status": cmd_status,
    "cancel": cmd_cancel,
    "publish": cmd_publish,
}


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return COMMANDS[args.command](args)
