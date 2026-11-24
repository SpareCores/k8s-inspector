"""The `fake` vendor: creates no cloud resources. For tests and for smoke runs of the controller flow.

`spec.network` (the ConfigMap's `dbaas.vendors.fake.networks.<region>`) may name an existing PostgreSQL
(`host`, `port`, `admin_user`, `admin_db`, `sslmode`; the password is the run's) that the benchmark then runs against.
Without one the host is a name that does not resolve, so the bootstrap fails the way an unreachable database does.
"""

import json
import os

from k8s_inspector_dbaas.spec import DbOutputs, DbSpec


def provision(spec: DbSpec) -> DbOutputs:
    network = spec.network
    return DbOutputs(
        host=network.get("host", f"{spec.run}.fake.invalid"),
        port=network.get("port", 5432),
        admin_user=network.get("admin_user", "scadmin"),
        admin_db=network.get("admin_db", "postgres"),
        sslmode=network.get("sslmode", "prefer"),
    )


def inventory() -> list[dict]:
    """The tagged databases of the fake cloud: the JSON list in `FAKE_DBAAS_INVENTORY`, none without it."""
    return json.loads(os.environ.get("FAKE_DBAAS_INVENTORY", "[]"))
