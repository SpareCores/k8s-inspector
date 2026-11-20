import importlib
import pkgutil

import k8s_inspector
from k8s_inspector import cli


def test_every_module_imports():
    names = [m.name for m in pkgutil.iter_modules(k8s_inspector.__path__)]
    assert {"model", "tasks", "storage", "pods", "crd", "runner", "planner", "publisher", "cli"} <= set(names)
    for name in names:
        importlib.import_module(f"k8s_inspector.{name}")


def test_cli_parses_run():
    args = cli.build_parser().parse_args(["run", "oci", "VM.Standard.E5.Flex.2-16", "--tasks", "a,b", "--hold", "2h"])
    assert args.tasks == ["a", "b"]
    assert args.hold.total_seconds() == 7200
