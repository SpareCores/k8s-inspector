# k8s-inspector-dbaas

The provisioner image of k8s-inspector: Python, the Pulumi CLI, the Pulumi SDK and one provider package per vendor.
The controller runs it as short-lived Jobs (`dbaas-up-<run>`, `dbaas-down-<run>` and the sweeper's `dbaas-inventory-<vendor>`) on the `apps` pool. See
[DBaaS](../docs/explanation/dbaas.md) for the design and [add a DBaaS vendor](../docs/how-to/add-dbaas-vendor.md)
for the vendor contract.

- `k8s_inspector_dbaas/spec.py`: the spec the controller passes and the outputs a vendor module returns.
- `k8s_inspector_dbaas/stack.py`: the Pulumi stack of one run (`up`, `down`); `__main__.py` is the Job entry point (`up`, `down`, `inventory`).
- `k8s_inspector_dbaas/storage_plan.py`: storage sizing that the vendor modules call.
- `k8s_inspector_dbaas/vendors/<vendor>.py`: one module per vendor; `fake.py` is for tests and smoke runs.

Development: `uv sync && uv run pytest`. The image installs the Pulumi CLI and the plugin of every installed
provider package (`install_plugins.py`), so a vendor only adds its package to the `vendors` dependency group (`uv add --group vendors <package>`).
