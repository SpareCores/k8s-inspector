"""The objects of a DBaaS run besides the BenchmarkRun: names, the spec handed to the provisioner, the Jobs
`dbaas-up-<run>` and `dbaas-down-<run>`, the Secret of the connection facts and the bootstrap Pod.

The Jobs run the image `k8s-inspector-dbaas` on the `apps` pool with no Kubernetes API access: the controller reads
their termination message. See docs/explanation/dbaas.md and docs/how-to/add-dbaas-vendor.md.
"""

import hashlib
import ipaddress
import json
import urllib.parse
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from importlib.resources import files

from k8s_inspector import crd, pods, tasks
from k8s_inspector.kube import Kube
from k8s_inspector.model import BenchmarkRun, Config, EnvConfig, OwnerRef, PodRole, TaskDef

# The controller and the provisioner image are separate packages that share no code, so the stack prefix is written in
# both (`k8s_inspector_dbaas.stack.STACK_PREFIX`); a test of this package compares the two.
STACK_PREFIX = "k8s-dbaas-"
STATE_PREFIX = "dbaas-state/"  # in the controller's bucket, below `K8SI_KEY_PREFIX`
STATE_PROJECT = "k8s-inspector-dbaas"  # the Pulumi project: `<prefix>.pulumi/stacks/<project>/<stack>.json`
PASSPHRASE_SECRET = "k8s-inspector-dbaas-state"  # key `passphrase`: the secrets provider of the stacks
DEFAULT_SERVICE_ACCOUNT = "k8s-inspector-dbaas"
LABEL_JOB = "inspector.sparecores.com/dbaas-job"  # on the Pods of a Job: its name
LABEL_DBAAS = "inspector.sparecores.com/dbaas"  # on Jobs: up | down
LABEL_SWEEP = "inspector.sparecores.com/dbaas-sweep"  # on a down Job that belongs to no BenchmarkRun
ANNOTATION_RUN = "inspector.sparecores.com/dbaas-run"  # on up and down Jobs: the full run name (the Job name is short)
CREDENTIALS_SECRET = "k8s-inspector-dbaas-{vendor}"  # optional: envFrom of the vendor's cloud credentials
AWS_TOKEN_DIR = "/var/run/secrets/aws"
STATE_PROFILE = "k8si-state"  # the AWS config profile of the state bucket: the vendor's providers never see its role
DOWN_DEADLINE_SECONDS = 2 * 3600  # one destroy attempt; the controller retries until it succeeds
INVENTORY_DEADLINE_SECONDS = 15 * 60
TERMINATION_GRACE_SECONDS = 15 * 60  # time for Pulumi to record what is in flight after SIGTERM
JOB_NAME_MAX = 63
PULUMI_TIMEOUT_MARGIN_MINUTES = 10  # the vendor module's create/delete timeouts end this long before the Job's deadline
BOOTSTRAP_BUDGET = timedelta(seconds=pods.BOOTSTRAP_SECONDS)
BOOTSTRAP_SCRIPT = "bootstrap.py"
RUN_PASSWORD_KEY = "K8SI_RUN_PASSWORD"  # the run's own password in Secret `<run>-db`; never overwritten
DB_KEYS = ("SC_DB_HOST", "SC_DB_PORT", "SC_DB_USER", "SC_DB_PASSWORD", "SC_DB_NAME", "SC_DB_SSLMODE", "PGSSLMODE")
WORKLOAD_DATABASE = "bench"  # created by the bootstrap; pgbench makes its own `pgbench` inside the server


# --------------------------------------------------------------------------- names


def stack_name(run: str) -> str:
    return f"{STACK_PREFIX}{run}"


def short_name(text: str, limit: int) -> str:
    """`text`, or when it is longer than `limit` its start and a hash of the whole (deterministic and unique)."""
    if len(text) <= limit:
        return text
    return f"{text[: limit - 9].rstrip('-')}-{hashlib.sha1(text.encode()).hexdigest()[:8]}"


def _job_name(prefix: str, text: str) -> str:
    return f"{prefix}{short_name(text, JOB_NAME_MAX - len(prefix))}"


def up_job_name(run: str) -> str:
    return _job_name("dbaas-up-", run)


def down_job_name(run: str) -> str:
    return _job_name("dbaas-down-", run)


def inventory_job_name(vendor: str) -> str:
    return _job_name("dbaas-inventory-", vendor)


def db_secret_name(run: str) -> str:
    """Secret `<run>-db`: the password first, the connection facts once the database exists."""
    return pods.db_secret_name(run)


def boot_configmap_name(run: str) -> str:
    return f"{run}-boot"


def boot_pod_name(run: str, attempt: int) -> str:
    return f"{pods.anchor_name(run, attempt)}-dbaas-boot"


def vendor_of_run(run: str, vendors: Iterable[str]) -> str | None:
    """The vendor of a run name (`<vendor>-<sku slug>-<crc32>`) among the known vendor ids; the longest match wins, so a
    vendor id may contain a dash. None for a run of an unknown vendor."""
    matches = [vendor for vendor in vendors if run.startswith(f"{vendor}-")]
    return max(matches, key=len, default=None)


def stack_state_key(run: str) -> str:
    """The key of the stack's state file in the bucket (below `K8SI_KEY_PREFIX`): Pulumi's DIY layout,
    `<state prefix>.pulumi/stacks/<project>/<stack>.json`."""
    return f"{STATE_PREFIX}.pulumi/stacks/{STATE_PROJECT}/{stack_name(run)}.json"


def state_history_prefix(run: str) -> str:
    """Where Pulumi keeps one file per update of the stack; the oldest is its creation."""
    return f"{STATE_PREFIX}.pulumi/history/{STATE_PROJECT}/{stack_name(run)}/"


def state_leftover_prefixes(run: str) -> list[str]:
    """Where Pulumi leaves files of the stack after a successful `destroy --remove`: its backups and history, and the
    `.bak` of the state. They hold no live resource, only old encrypted state, so the controller deletes them."""
    stack = stack_name(run)
    base = f"{STATE_PREFIX}.pulumi"
    return [
        f"{base}/backups/{STATE_PROJECT}/{stack}/",
        state_history_prefix(run),
        f"{base}/stacks/{STATE_PROJECT}/{stack}.json",
    ]


def run_of_state_key(key: str) -> str | None:
    """The run name of a stack file key (`dbaas-state/.pulumi/stacks/<project>/k8s-dbaas-<run>.json`), else None
    (backups, history, `.bak` and the other files of the layout are not stacks)."""
    prefix = f"{STATE_PREFIX}.pulumi/stacks/{STATE_PROJECT}/{STACK_PREFIX}"
    if not (key.startswith(prefix) and key.endswith(".json")):
        return None
    return key.removeprefix(prefix).removesuffix(".json") or None


def clean_state(storage, run: str) -> None:
    """Delete what Pulumi left in the bucket for the stack of the run (see `state_leftover_prefixes`)."""
    leftovers = []
    for prefix in state_leftover_prefixes(run):
        leftovers += [key for key, _ in storage.list_objects(prefix)]
    storage.delete_objects(leftovers)


def backend_url(env: EnvConfig) -> str:
    """The Pulumi DIY backend URL of the state: the controller's bucket, `dbaas-state/` below `K8SI_KEY_PREFIX`. With a
    role for the Jobs (`K8SI_DBAAS_ROLE_ARN`) the URL names the AWS profile `STATE_PROFILE` (see `state_aws_config`), so
    the state credentials are not the default ones of the process, which the vendor's providers would pick up."""
    key_prefix = env.key_prefix.strip("/")
    path = f"{key_prefix + '/' if key_prefix else ''}{STATE_PREFIX}".rstrip("/")
    query = {"region": env.bucket_region, "awssdk": "v2"}
    if env.dbaas_role_arn:
        query["profile"] = STATE_PROFILE
    if env.s3_endpoint_url:
        query |= {"endpoint": env.s3_endpoint_url, "use_path_style": "true"}
    return f"s3://{env.bucket}/{path}?{urllib.parse.urlencode(query)}"


def state_aws_config(env: EnvConfig) -> str | None:
    """The AWS config file of the Jobs: one profile, the web identity of the state bucket's role. None without a role.
    The provisioner writes it to a file and points `AWS_CONFIG_FILE` at it; no `AWS_ROLE_ARN`, `AWS_REGION` or the like
    is set, so a vendor's AWS provider does not get the state credentials."""
    if not env.dbaas_role_arn:
        return None
    return (
        f"[profile {STATE_PROFILE}]\n"
        f"role_arn = {env.dbaas_role_arn}\n"
        f"web_identity_token_file = {AWS_TOKEN_DIR}/token\n"
        f"region = {env.bucket_region}\n"
    )


# --------------------------------------------------------------------------- the spec


def _stamp(when: datetime) -> str:
    return when.astimezone(UTC).strftime("%Y%m%dt%H%Mz")


def expiry(now: datetime, cfg: Config, task: TaskDef) -> datetime:
    """When the database is a leak: its creation, the benchmark, the bootstrap and a destroy attempt, all with their
    deadlines, have passed."""
    create = timedelta(minutes=cfg.settings.dbaas.create_minutes)
    return now + create + BOOTSTRAP_BUDGET + task.timeout + timedelta(hours=1)


def labels(run_id: str, expires: datetime) -> dict[str, str]:
    """Run id and expiry as label-safe strings (lowercase, digits, `-`, `_`), valid as every vendor's tag or label."""
    return {"sc_inspector_run": run_id.lower(), "sc_inspector_expires": _stamp(expires)}


def client_addresses(node: dict) -> list[str]:
    """The ExternalIP addresses of the client Node as `/32` and `/128` networks, for the allowlist of a vendor whose
    database is on a public endpoint. Empty when the Node has none."""
    found = (a["address"] for a in node.get("status", {}).get("addresses") or [] if a.get("type") == "ExternalIP")
    return [ipaddress.ip_network(address).with_prefixlen for address in found]


def build_spec(
    run: BenchmarkRun, cfg: Config, record: dict, network: dict, expires: datetime, addresses: list[str]
) -> dict:
    """`DBAAS_SPEC` of the provisioner (`k8s_inspector_dbaas.spec.DbSpec` without the password). The Pulumi timeouts
    the vendor module must set are shorter than the Jobs' deadlines, so Pulumi fails cleanly and records what it did.
    `network` is the ConfigMap's entry of the client's region, `addresses` the client node's public ones."""
    db = run.spec.database
    return {
        "vendor": db["vendor"],
        "databaseId": db["id"],
        "engineVersion": str(db["engineVersion"]),
        "region": db["region"],
        "clientRegion": pods.client_region(db),
        "zone": run.status.zone,
        "run": run.name,
        "runId": run.spec.run_id,
        "expiresAt": expires.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "labels": labels(run.spec.run_id, expires),
        "createTimeoutMinutes": max(cfg.settings.dbaas.create_minutes - PULUMI_TIMEOUT_MARGIN_MINUTES, 1),
        "deleteTimeoutMinutes": max(DOWN_DEADLINE_SECONDS // 60 - PULUMI_TIMEOUT_MARGIN_MINUTES, 1),
        "network": network,
        "clientAddresses": addresses,
        "record": record,
    }


def connection_secret(run: BenchmarkRun, owner: OwnerRef, password: str, outputs: dict | None) -> dict:
    """Secret `<run>-db`. With `outputs` (the provisioner's connection facts) it carries the `SC_DB_*` variables of the
    benchmark and the bootstrap, and `SC_DB_PASSWORD`, the password the benchmark uses: the vendor's when it generated
    the admin password, else the run's. `K8SI_RUN_PASSWORD`, the run's own password the Jobs get, is always there."""
    data = {RUN_PASSWORD_KEY: password}
    if outputs:
        data["SC_DB_PASSWORD"] = outputs.get("password") or password
        user = outputs.get("user") or outputs["admin_user"]
        sslmode = outputs.get("sslmode", "require")
        data |= {
            "SC_DB_HOST": outputs["host"],
            "SC_DB_PORT": str(outputs.get("port", 5432)),
            "SC_DB_USER": user,
            "SC_DB_NAME": WORKLOAD_DATABASE,
            "SC_DB_SSLMODE": sslmode,
            "PGSSLMODE": sslmode,
            "SC_DB_BOOTSTRAP_USER": outputs["admin_user"],
            "SC_DB_BOOTSTRAP_DATABASE": outputs["admin_db"],
            "SC_DB_GRANT_ROLE": outputs.get("grant_role") or "",
        }
    return {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": db_secret_name(run.name),
            "namespace": run.namespace,
            "labels": {crd.LABEL_RUN: run.name, pods.LABEL_ROLE: PodRole.DBAAS.value},
            "ownerReferences": pods._owner_refs(owner),
        },
        "type": "Opaque",
        "stringData": data,
    }


# --------------------------------------------------------------------------- Jobs


def job_state(job: dict) -> str:
    """`complete`, `failed` or `running`."""
    for condition in job.get("status", {}).get("conditions") or []:
        if condition.get("status") == "True" and condition.get("type") in ("Complete", "Failed"):
            return condition["type"].lower()
    return "running"


def termination_message(pod: dict) -> dict:
    """The JSON in the termination message of the Pod's container (how Jobs and the bootstrap report); empty when there
    is none. A message that is not a JSON object counts as an error text."""
    for status in pod.get("status", {}).get("containerStatuses") or []:
        text = status.get("state", {}).get("terminated", {}).get("message")
        if text:
            try:
                message = json.loads(text)
            except ValueError:
                return {"error": text[:500]}
            return message if isinstance(message, dict) else {}
    return {}


def job_message(kube: Kube, job_name: str) -> dict:
    """`termination_message` of the Pod of the Job."""
    for pod in kube.list("pod", labels={LABEL_JOB: job_name}):
        message = termination_message(pod)
        if message:
            return message
    return {}


def _state_env(env_cfg: EnvConfig) -> list[dict]:
    """The Pulumi state backend: bucket URL, secrets passphrase and, with a role, the AWS profile text."""
    env = [
        {"name": "PULUMI_BACKEND_URL", "value": backend_url(env_cfg)},
        {
            "name": "PULUMI_CONFIG_PASSPHRASE",
            "valueFrom": {"secretKeyRef": {"name": PASSPHRASE_SECRET, "key": "passphrase"}},
        },
    ]
    aws_config = state_aws_config(env_cfg)
    return env + ([{"name": "K8SI_STATE_AWS_CONFIG", "value": aws_config}] if aws_config else [])


def _job(
    name: str,
    command: str,
    *,
    kind: str,
    vendor: str,
    cfg: Config,
    env: list[dict],
    deadline_seconds: int,
    run: str | None,
    owner: OwnerRef | None,
    state: bool = True,
) -> dict:
    """A Job with one Pod of the provisioner image: no retries inside the Job (the controller decides), no Kubernetes
    API token, the vendor's credentials from an optional Secret. With `state` it also gets the Pulumi state bucket
    backend; the projected web identity token (`AWS_TOKEN_DIR/token`, audience `sts.amazonaws.com`) is mounted in every
    Job when `K8SI_DBAAS_ROLE_ARN` is set. The Pod is never disrupted by Karpenter and has time to let Pulumi
    record what is in flight when it is stopped."""
    env_cfg = cfg.env
    vendor_config = cfg.settings.dbaas.vendors.get(vendor)
    job_labels = {LABEL_DBAAS: kind, crd.LABEL_VENDOR: vendor, pods.LABEL_ROLE: PodRole.DBAAS.value}
    if run:
        job_labels[crd.LABEL_RUN] = run
    container_env = [*(_state_env(env_cfg) if state else []), *env]
    mounts = [{"name": "work", "mountPath": "/work"}, {"name": "tmp", "mountPath": "/tmp"}]
    volumes = [{"name": "work", "emptyDir": {"sizeLimit": "1Gi"}}, {"name": "tmp", "emptyDir": {"sizeLimit": "512Mi"}}]
    if env_cfg.dbaas_role_arn:  # every Job kind: vendor modules may use the token too (the state env is state-only)
        mounts.append({"name": "aws-token", "mountPath": AWS_TOKEN_DIR, "readOnly": True})
        token = {"serviceAccountToken": {"audience": "sts.amazonaws.com", "expirationSeconds": 3600, "path": "token"}}
        volumes.append({"name": "aws-token", "projected": {"sources": [token]}})
    metadata = {"name": name, "namespace": cfg.env.namespace, "labels": job_labels}
    if owner:
        metadata["ownerReferences"] = pods._owner_refs(owner)
    if run:
        metadata["annotations"] = {ANNOTATION_RUN: run}
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": metadata,
        "spec": {
            "backoffLimit": 0,
            "activeDeadlineSeconds": deadline_seconds,
            "template": {
                "metadata": {
                    "labels": {LABEL_JOB: name, pods.LABEL_ROLE: PodRole.DBAAS.value},
                    "annotations": {"karpenter.sh/do-not-disrupt": "true"},
                },
                "spec": {
                    "restartPolicy": "Never",
                    "terminationGracePeriodSeconds": TERMINATION_GRACE_SECONDS,
                    "serviceAccountName": (vendor_config and vendor_config.service_account) or DEFAULT_SERVICE_ACCOUNT,
                    "automountServiceAccountToken": False,
                    "enableServiceLinks": False,
                    "nodeSelector": {"karpenter.sh/nodepool": "apps"},
                    "tolerations": [{"key": "frankenscube/apps", "operator": "Exists"}],
                    "securityContext": {
                        "runAsNonRoot": True,
                        "runAsUser": 10001,
                        "runAsGroup": 10001,
                        "fsGroup": 10001,
                        "seccompProfile": {"type": "RuntimeDefault"},
                    },
                    "containers": [
                        {
                            "name": "provisioner",
                            "image": env_cfg.dbaas_image,
                            "args": [command],
                            "env": container_env,
                            "envFrom": [
                                {"secretRef": {"name": CREDENTIALS_SECRET.format(vendor=vendor), "optional": True}}
                            ],
                            "resources": {"requests": {"cpu": "100m", "memory": "512Mi"}, "limits": {"memory": "2Gi"}},
                            "securityContext": {"allowPrivilegeEscalation": False, "capabilities": {"drop": ["ALL"]}},
                            "terminationMessagePolicy": "File",
                            "volumeMounts": mounts,
                        }
                    ],
                    "volumes": volumes,
                },
            },
        },
    }


def _run_password_env(run_name: str, *, optional: bool) -> dict:
    ref = {"name": db_secret_name(run_name), "key": RUN_PASSWORD_KEY}
    return {"name": "DBAAS_PASSWORD", "valueFrom": {"secretKeyRef": ref | ({"optional": True} if optional else {})}}


def up_job(run: BenchmarkRun, cfg: Config, spec: dict, owner: OwnerRef) -> dict:
    """`dbaas-up-<run>`: create the database. The spec is not secret, the password comes from the run's Secret. The
    Job's deadline is `dbaas.createMinutes`; the vendor module's Pulumi timeouts are shorter (`build_spec`)."""
    env = [
        {"name": "DBAAS_SPEC", "value": json.dumps(spec, separators=(",", ":"))},
        _run_password_env(run.name, optional=False),
    ]
    return _job(
        up_job_name(run.name),
        "up",
        kind="up",
        vendor=run.spec.vendor,
        cfg=cfg,
        env=env,
        deadline_seconds=cfg.settings.dbaas.create_minutes * 60,
        run=run.name,
        owner=owner,
    )


def down_job(run_name: str, vendor: str, cfg: Config, *, sweep: bool = False) -> dict:
    """`dbaas-down-<run>`: destroy the stack and remove its state. It is labelled with the run, not owned by it (the run
    waits for it under foreground deletion), and the one that finishes it deletes it. A `sweep` Job is for a stack whose
    run is gone: it carries no run label, and the sweeper deletes it when it is over. The run's password is optional
    here: `down` only uses it to mask error text, and the Secret or its key may be missing (a leftover stack, a lost
    Secret, a run from before the key); a required reference would stall the Pod and never destroy the database."""
    job = _job(
        down_job_name(run_name),
        "down",
        kind="down",
        vendor=vendor,
        cfg=cfg,
        env=[{"name": "DBAAS_RUN", "value": run_name}, {"name": "DBAAS_VENDOR", "value": vendor}]
        + ([] if sweep else [_run_password_env(run_name, optional=True)]),
        deadline_seconds=DOWN_DEADLINE_SECONDS,
        run=None if sweep else run_name,
        owner=None,
    )
    job["metadata"].setdefault("annotations", {})[ANNOTATION_RUN] = run_name
    if sweep:
        job["metadata"]["labels"][LABEL_SWEEP] = "true"
    return job


def inventory_job(vendor: str, cfg: Config) -> dict:
    """`dbaas-inventory-<vendor>`: list the vendor's databases that carry the `sc_inspector_run` tag, with the vendor's
    credentials and ServiceAccount like `up` and `down` (and no state bucket). `DBAAS_REGIONS` is the comma-separated,
    sorted regions the platform configured for the vendor (`dbaas.vendors.<vendor>.networks`): the only ones with
    databases."""
    vendor_config = cfg.settings.dbaas.vendors.get(vendor)
    regions = ",".join(sorted(vendor_config.networks)) if vendor_config else ""
    return _job(
        inventory_job_name(vendor),
        "inventory",
        kind="inventory",
        vendor=vendor,
        cfg=cfg,
        env=[{"name": "DBAAS_VENDOR", "value": vendor}, {"name": "DBAAS_REGIONS", "value": regions}],
        deadline_seconds=INVENTORY_DEADLINE_SECONDS,
        run=None,
        owner=None,
        state=False,
    )


# --------------------------------------------------------------------------- the bootstrap Pod


def bootstrap_script() -> str:
    """The text of `dbaas_bootstrap.py`, which runs in the benchmark image."""
    return files("k8s_inspector").joinpath("dbaas_bootstrap.py").read_text()


def boot_configmap(run: BenchmarkRun, owner: OwnerRef) -> dict:
    return {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": boot_configmap_name(run.name),
            "namespace": run.namespace,
            "labels": {crd.LABEL_RUN: run.name, pods.LABEL_ROLE: PodRole.DBAAS.value},
            "ownerReferences": pods._owner_refs(owner),
        },
        "data": {BOOTSTRAP_SCRIPT: bootstrap_script()},
    }


def boot_pod(run: BenchmarkRun, attempt: int, cfg: Config, task: TaskDef, *, node_name: str, owner: OwnerRef) -> dict:
    """The Pod that prepares the database from the client node: the benchmark image (it has Python and psycopg) runs the
    bootstrap script from the ConfigMap, with the connection facts from the run's Secret."""
    db = run.spec.database
    env = [
        {"name": "DB_WAIT_TIMEOUT_SEC", "value": str(pods.BOOTSTRAP_WAIT_SECONDS)},
        {"name": "SC_DB_VCPUS", "value": str(db["vcpus"])},
        {"name": "SC_DB_MEM_GIB", "value": str(db["memoryGib"])},
        *({"name": k, "value": v} for k, v in task.env),
    ]
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": boot_pod_name(run.name, attempt),
            "namespace": cfg.env.namespace,
            "labels": pods.labels(run, PodRole.DBAAS, attempt),
            "ownerReferences": pods._owner_refs(owner),
        },
        "spec": {
            "nodeName": node_name,
            "restartPolicy": "Never",
            "activeDeadlineSeconds": int(BOOTSTRAP_BUDGET.total_seconds()),
            "hostNetwork": True,
            "dnsPolicy": "Default",
            "automountServiceAccountToken": False,
            "enableServiceLinks": False,
            "tolerations": [{"operator": "Exists"}],
            "containers": [
                {
                    "name": "bootstrap",
                    "image": tasks.image_ref(task),
                    "command": ["python3", f"/dbaas/{BOOTSTRAP_SCRIPT}"],
                    "env": env,
                    "envFrom": [{"secretRef": {"name": db_secret_name(run.name)}}],
                    "resources": {"requests": {"cpu": "10m", "memory": "0"}},
                    "terminationMessagePolicy": "File",
                    "volumeMounts": [
                        {"name": "script", "mountPath": "/dbaas", "readOnly": True},
                        {"name": "k8si", "mountPath": pods.MOUNT_DIR},
                    ],
                }
            ],
            "volumes": [
                {"name": "script", "configMap": {"name": boot_configmap_name(run.name)}},
                {"name": "k8si", "hostPath": {"path": pods.NODE_DIR, "type": "Directory"}},
            ],
        },
    }
