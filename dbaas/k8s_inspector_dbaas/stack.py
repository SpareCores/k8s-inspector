"""The Pulumi stack of one run: `up` creates the database, `down` destroys it and removes the state.

One stack per run, named `k8s-dbaas-<run>` (`stack_name`), in the project `k8s-inspector-dbaas`, in the DIY backend
that `PULUMI_BACKEND_URL` names (the controller's bucket, prefix `dbaas-state/`). The passphrase of the secrets
provider is `PULUMI_CONFIG_PASSPHRASE`. The controller derives the stack name from the run name alone, so after a
restart it can always run `down`, and it recognises leaked stacks by that prefix.
"""

import contextlib
import os
from collections.abc import Callable

from pulumi import automation as auto

from k8s_inspector_dbaas import vendors
from k8s_inspector_dbaas.spec import DbOutputs, DbSpec

PROJECT = "k8s-inspector-dbaas"
STACK_PREFIX = "k8s-dbaas-"


def stack_name(run: str) -> str:
    return f"{STACK_PREFIX}{run}"


def _options(program: Callable[[], None] | None) -> auto.LocalWorkspaceOptions:
    return auto.LocalWorkspaceOptions(
        program=program,
        project_settings=auto.ProjectSettings(
            name=PROJECT, runtime="python", backend=auto.ProjectBackend(os.environ["PULUMI_BACKEND_URL"])
        ),
        secrets_provider="passphrase",
    )


def _program(spec: DbSpec) -> Callable[[], None]:
    provision = vendors.load(spec.vendor).provision  # a missing vendor module fails here, not inside Pulumi

    def program() -> None:
        provision(spec).export()

    return program


def up(spec: DbSpec, on_output: Callable[[str], None] = print) -> DbOutputs:
    """Create (or finish creating) the database of the run and return its connection facts."""
    program = _program(spec)
    stack = auto.create_or_select_stack(
        stack_name(spec.run), project_name=PROJECT, program=program, opts=_options(program)
    )
    result = stack.up(on_output=on_output, show_secrets=False)
    return DbOutputs.from_exports({name: output.value for name, output in result.outputs.items()})


def down(run: str, on_output: Callable[[str], None] = print) -> None:
    """Destroy everything of the run and remove its state (only when the destroy succeeded). A run without a stack
    has nothing to destroy. The destroy follows `cancel` (a killed `up` leaves a lock) and refreshes the state first
    (so a half-created resource that exists is destroyed, and one that does not is dropped)."""
    try:
        stack = auto.select_stack(stack_name(run), project_name=PROJECT, program=lambda: None, opts=_options(None))
    except auto.StackNotFoundError:
        on_output(f"no stack {stack_name(run)}, nothing to destroy\n")
        return
    with contextlib.suppress(auto.CommandError):  # nothing was running
        stack.cancel()
    stack.destroy(on_output=on_output, refresh=True, remove=True, show_secrets=False)
