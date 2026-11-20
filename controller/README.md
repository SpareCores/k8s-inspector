# k8s-inspector controller

The Python controller (`k8s-inspector serve | plan | run | status | cancel | publish`). Design:
[docs](../docs/index.md); each module's docstring says which document it implements.

Requires Python >= 3.15 and [uv](https://docs.astral.sh/uv/).

```sh
cd controller
uv sync
uv run pytest
uv run ruff check . && uv run ruff format --check .
uv run k8s-inspector --help
```

Layout: `k8s_inspector/` is the package (`model.py` holds the shared dataclasses, one module per concern),
`tests/` the tests, `k8s_inspector/catalogue/tasks.yaml` the task catalogue. Tests use fakes and never touch a
cluster or a cloud.
