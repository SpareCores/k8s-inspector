"""The vendor contract: what the controller passes in (`DbSpec`) and what a vendor module returns (`DbOutputs`).

A vendor module `vendors/<vendor>.py` exposes one function, `provision(spec: DbSpec) -> DbOutputs`. It runs inside
a Pulumi program: it declares the resources of one database (and its user and firewall entry) and returns the
connection facts. Free prerequisites (network, subnets, DNS zones) already exist and are described by `spec.network`.
"""

import json
import os
from dataclasses import dataclass, field, fields

import pulumi

PASSWORD_ENV = "DBAAS_PASSWORD"
SPEC_ENV = "DBAAS_SPEC"


@dataclass(frozen=True)
class DbSpec:
    """The database to create. Everything but `password` comes from the controller's JSON (`DBAAS_SPEC`)."""

    vendor: str
    database_id: str  # Keeper's `api_reference` of the database SKU, the vendor's own name for it
    engine_version: str  # PostgreSQL major version, e.g. "18"
    region: str  # the region of the client node (Keeper/SCarpenter `api_reference`)
    zone: str | None  # the zone of the client node, None when unknown; a vendor may place the database in it
    run: str  # BenchmarkRun name
    run_id: str  # `spec.runId` of the run
    expires_at: str  # RFC 3339 UTC: after this the database is a leak (the controller sweeps stacks after 6 h)
    labels: dict[str, str]  # run id and expiry as label-safe strings; apply them as tags/labels where supported
    network: dict  # opaque infrastructure outputs from the ConfigMap `dbaas.vendors.<vendor>.networks.<region>`
    record: dict  # Keeper's record of the database (vcpus, memory_amount, storage_size, api_reference_object ...)
    password: str = field(repr=False, default="")  # admin password, from `DBAAS_PASSWORD`; a Pulumi secret

    @property
    def vcpus(self) -> int:
        return int(self.record["vcpus"])

    @property
    def memory_gib(self) -> float:
        return float(self.record["memory_amount"]) / 1024  # Keeper stores MiB

    def password_output(self) -> pulumi.Output[str]:
        """The password as a Pulumi secret. Use this, never `password`, in a resource, or it lands in the state
        in plain text."""
        return pulumi.Output.secret(self.password)


@dataclass(frozen=True)
class DbOutputs:
    """Connection facts the controller writes to the run's Secret and the bootstrap uses. A field may be a Pulumi
    output while the program runs; `export` registers them as stack outputs."""

    host: pulumi.Input[str]
    admin_user: pulumi.Input[str]  # the user the vendor created; the bootstrap connects as this one
    admin_db: pulumi.Input[
        str
    ]  # a database that exists from the start ("postgres", "defaultdb"); the bootstrap uses it
    port: pulumi.Input[int] = 5432
    sslmode: pulumi.Input[str] = "require"  # libpq sslmode of the client
    user: pulumi.Input[str] | None = None  # workload user; None: `admin_user`. Otherwise the bootstrap creates it
    grant_role: pulumi.Input[str] | None = None  # role the bootstrap grants to `user` (Cloud SQL: "cloudsqlsuperuser")

    def export(self) -> None:
        """Register every set field as a stack output (inside the Pulumi program)."""
        for f in fields(self):
            value = getattr(self, f.name)
            if value is not None:
                pulumi.export(f.name, value)

    @classmethod
    def from_exports(cls, exports: dict) -> DbOutputs:
        """The outputs of a finished `up` (plain values)."""
        return cls(**{f.name: exports[f.name] for f in fields(cls) if f.name in exports})

    def to_json(self) -> str:
        return json.dumps({f.name: getattr(self, f.name) for f in fields(self) if getattr(self, f.name) is not None})


def load_spec(environ: dict[str, str] | None = None) -> DbSpec:
    """The spec from `DBAAS_SPEC` (JSON) and `DBAAS_PASSWORD`. Raise `KeyError` or `ValueError` when it is invalid."""
    environ = os.environ if environ is None else environ
    data = json.loads(environ[SPEC_ENV])
    return DbSpec(
        vendor=data["vendor"],
        database_id=data["databaseId"],
        engine_version=str(data["engineVersion"]),
        region=data["region"],
        zone=data.get("zone"),
        run=data["run"],
        run_id=data["runId"],
        expires_at=data["expiresAt"],
        labels=dict(data["labels"]),
        network=dict(data.get("network") or {}),
        record=dict(data["record"]),
        password=environ.get(PASSWORD_ENV, ""),
    )
