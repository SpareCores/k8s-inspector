"""The vendor contract: what the controller passes in (`DbSpec`) and what a vendor module returns (`DbOutputs`).

A vendor module `vendors/<vendor>.py` exposes two functions:

- `provision(spec: DbSpec) -> DbOutputs` runs inside a Pulumi program: it declares the resources of one database (and
  its user and firewall entry) and returns the connection facts. Free prerequisites (network, subnets, DNS zones)
  already exist and are described by `spec.network`.
- `inventory() -> list[dict]` lists the vendor's databases that carry the `sc_inspector_run` tag or label, outside
  Pulumi, as `{"id", "run", "expires_at", "created_at"}` (RFC 3339 times): how a database that no state records is
  found. It reads the vendor's credentials from the environment, like `provision`.

Rules for `provision` that make `down` work from the state alone:

- Create an explicit provider with `region=spec.region`; take its credentials from the environment, never as provider
  arguments (those are stored in the state and go stale when a key is rotated).
- Set `ResourceOptions(custom_timeouts=...)` with `spec.create_timeout` and `spec.delete_timeout` on every slow
  resource, so Pulumi fails by itself before the Job's deadline and records the resource.
- Name every resource deterministically (from `spec.run`), so a retry finds what an earlier attempt created.
"""

import json
import os
from dataclasses import dataclass, field, fields

import pulumi

from k8s_inspector_dbaas import sealing

PASSWORD_ENV = "DBAAS_PASSWORD"
SPEC_ENV = "DBAAS_SPEC"


@dataclass(frozen=True)
class DbSpec:
    """The database to create. Everything but `password` comes from the controller's JSON (`DBAAS_SPEC`)."""

    vendor: str
    database_id: str  # Keeper's `api_reference` of the database SKU, the vendor's own name for it
    engine_version: str  # PostgreSQL major version, e.g. "18"
    region: str  # the region of the database (Keeper `api_reference`)
    client_region: (
        str  # the region of the client node: `region`, or a region of its city when the vendor's database and
    )
    # server regions differ (OVH); the key of `network`
    zone: str | None  # the zone of the client node, None when unknown; a vendor may place the database in it
    run: str  # BenchmarkRun name
    run_id: str  # `spec.runId` of the run
    expires_at: str  # RFC 3339 UTC: after this the database is a leak (the controller sweeps stacks after 6 h)
    labels: dict[str, str]  # run id and expiry as label-safe strings; apply them as tags/labels where supported
    create_timeout_minutes: int  # shorter than the up Job's deadline: the `create` custom timeout of slow resources
    delete_timeout_minutes: int  # shorter than the down Job's deadline: the `delete` custom timeout
    network: (
        dict  # opaque outputs of the platform from the ConfigMap `dbaas.vendors.<vendor>.networks.<client region>`;
    )
    # for a vendor that needs an allowlist on a private network (UpCloud, Vultr, OCI) the platform puts the client CIDRs
    # in it
    record: dict  # Keeper's record of the database (vcpus, memory_amount, storage_size, api_reference_object ...)
    # ExternalIP addresses of the client node as /32 or /128, for a vendor whose nodes have no private network (OVH):
    # the database is on a public endpoint with TLS and allows these. Empty when the node has none.
    client_addresses: list[str] = field(default_factory=list)
    password: str = field(repr=False, default="")  # admin password, from `DBAAS_PASSWORD`; a Pulumi secret

    @property
    def vcpus(self) -> int:
        return int(self.record["vcpus"])

    @property
    def create_timeout(self) -> str:
        """For `ResourceOptions(custom_timeouts=pulumi.CustomTimeouts(create=...))`."""
        return f"{self.create_timeout_minutes}m"

    @property
    def delete_timeout(self) -> str:
        return f"{self.delete_timeout_minutes}m"

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
    # The admin password when the vendor generates it and cannot be given one (OVH): a Pulumi secret that replaces the
    # run's. It never leaves the Job in plain text (`to_json` seals it).
    password: pulumi.Input[str] | None = field(default=None, repr=False)

    def export(self) -> None:
        """Register every set field as a stack output (inside the Pulumi program)."""
        for f in fields(self):
            value = getattr(self, f.name)
            if value is not None:
                pulumi.export(f.name, pulumi.Output.secret(value) if f.name == "password" else value)

    @classmethod
    def from_exports(cls, exports: dict) -> DbOutputs:
        """The outputs of a finished `up` (plain values)."""
        return cls(**{f.name: exports[f.name] for f in fields(cls) if f.name in exports})

    def to_json(self, run_password: str = "") -> str:
        """The termination message. A vendor-generated `password` is sealed with `run_password` (`sealing`): the message
        is readable by anyone who can read Pods."""
        facts = {f.name: getattr(self, f.name) for f in fields(self) if getattr(self, f.name) is not None}
        if "password" in facts:
            if not run_password:
                raise ValueError("a vendor-generated password needs the run's password to be sealed with")
            facts["password"] = sealing.seal(run_password, facts["password"])
        return json.dumps(facts)


def load_spec(environ: dict[str, str] | None = None) -> DbSpec:
    """The spec from `DBAAS_SPEC` (JSON) and `DBAAS_PASSWORD`. Raise `KeyError` or `ValueError` when it is invalid."""
    environ = os.environ if environ is None else environ
    data = json.loads(environ[SPEC_ENV])
    return DbSpec(
        vendor=data["vendor"],
        database_id=data["databaseId"],
        engine_version=str(data["engineVersion"]),
        region=data["region"],
        client_region=data.get("clientRegion") or data["region"],
        zone=data.get("zone"),
        run=data["run"],
        run_id=data["runId"],
        expires_at=data["expiresAt"],
        labels=dict(data["labels"]),
        create_timeout_minutes=int(data["createTimeoutMinutes"]),
        delete_timeout_minutes=int(data["deleteTimeoutMinutes"]),
        network=dict(data.get("network") or {}),
        record=dict(data["record"]),
        client_addresses=list(data.get("clientAddresses") or []),
        password=environ.get(PASSWORD_ENV, ""),
    )
