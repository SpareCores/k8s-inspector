"""The objects of a DBaaS run besides the BenchmarkRun: names, the spec handed to the provisioner, the Jobs
`dbaas-up-<run>` and `dbaas-down-<run>`, the Secret of the connection facts and the bootstrap Pod.

The Jobs run the image `k8s-inspector-dbaas` on the `apps` pool with no Kubernetes API access: the controller reads
their termination message. See docs/explanation/dbaas.md and docs/how-to/add-dbaas-vendor.md.
"""

import json
import urllib.parse
from datetime import UTC, datetime, timedelta
from importlib.resources import files

from k8s_inspector import crd, pods, tasks
from k8s_inspector.model import BenchmarkRun, Config, EnvConfig, OwnerRef, PodRole, TaskDef

STACK_PREFIX = "k8s-dbaas-"  # keep equal to `k8s_inspector_dbaas.stack.STACK_PREFIX`
STATE_PREFIX = "dbaas-state/"  # in the controller's bucket, below `K8SI_KEY_PREFIX`
STATE_PROJECT = "k8s-inspector-dbaas"  # the Pulumi project: `<prefix>.pulumi/stacks/<project>/<stack>.json`
PASSPHRASE_SECRET = "k8s-inspector-dbaas-state"  # key `passphrase`: the secrets provider of the stacks
DEFAULT_SERVICE_ACCOUNT = "k8s-inspector-dbaas"
LABEL_JOB = "inspector.sparecores.com/dbaas-job"  # on the Pods of a Job: its name
LABEL_DBAAS = "inspector.sparecores.com/dbaas"  # on Jobs: up | down
LABEL_SWEEP = "inspector.sparecores.com/dbaas-sweep"  # on a down Job that belongs to no BenchmarkRun
CREDENTIALS_SECRET = "k8s-inspector-dbaas-{vendor}"  # optional: envFrom of the vendor's cloud credentials
AWS_TOKEN_DIR = "/var/run/secrets/aws"
DOWN_DEADLINE_SECONDS = 2 * 3600  # one destroy attempt; the controller retries until it succeeds
BOOTSTRAP_BUDGET = timedelta(seconds=pods.BOOTSTRAP_SECONDS)
BOOTSTRAP_SCRIPT = "bootstrap.py"
DB_KEYS = ("SC_DB_HOST", "SC_DB_PORT", "SC_DB_USER", "SC_DB_PASSWORD", "SC_DB_NAME", "SC_DB_SSLMODE", "PGSSLMODE")
WORKLOAD_DATABASE = "bench"  # created by the bootstrap; pgbench makes its own `pgbench` inside the server


# --------------------------------------------------------------------------- names


def stack_name(run: str) -> str:
    return f"{STACK_PREFIX}{run}"


def up_job_name(run: str) -> str:
    return f"dbaas-up-{run}"


def down_job_name(run: str) -> str:
    return f"dbaas-down-{run}"


def db_secret_name(run: str) -> str:
    """Secret `<run>-db`: the password first, the connection facts once the database exists."""
    return pods.db_secret_name(run)


def boot_configmap_name(run: str) -> str:
    return f"{run}-boot"


def boot_pod_name(run: str, attempt: int) -> str:
    return f"{pods.anchor_name(run, attempt)}-dbaas-boot"


def vendor_of_run(run: str) -> str:
    """A run name is `<vendor>-<sku slug>-<crc32>`; vendor ids have no dash."""
    return run.split("-", 1)[0]


def stack_state_key(run: str) -> str:
    """The key of the stack's state file in the bucket (below `K8SI_KEY_PREFIX`): Pulumi's DIY layout,
    `<state prefix>.pulumi/stacks/<project>/<stack>.json`."""
    return f"{STATE_PREFIX}.pulumi/stacks/{STATE_PROJECT}/{stack_name(run)}.json"


def state_leftover_prefixes(run: str) -> list[str]:
    """Where Pulumi leaves files of the stack after a successful `destroy --remove`: its backups and history, and the
    `.bak` of the state. They hold no live resource, only old encrypted state, so the controller deletes them."""
    stack = stack_name(run)
    base = f"{STATE_PREFIX}.pulumi"
    return [
        f"{base}/backups/{STATE_PROJECT}/{stack}/",
        f"{base}/history/{STATE_PROJECT}/{stack}/",
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
    """The Pulumi DIY backend URL of the state: the controller's bucket, `dbaas-state/` below `K8SI_KEY_PREFIX`."""
    key_prefix = env.key_prefix.strip("/")
    path = f"{key_prefix + '/' if key_prefix else ''}{STATE_PREFIX}".rstrip("/")
    query = {"region": env.bucket_region}
    if env.s3_endpoint_url:
        query |= {"endpoint": env.s3_endpoint_url, "s3ForcePathStyle": "true"}
    return f"s3://{env.bucket}/{path}?{urllib.parse.urlencode(query)}"


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


def build_spec(run: BenchmarkRun, record: dict, network: dict, expires: datetime) -> dict:
    """`DBAAS_SPEC` of the provisioner (`k8s_inspector_dbaas.spec.DbSpec` without the password)."""
    db = run.spec.database
    return {
        "vendor": db["vendor"],
        "databaseId": db["id"],
        "engineVersion": str(db["engineVersion"]),
        "region": db["region"],
        "zone": run.status.zone,
        "run": run.name,
        "runId": run.spec.run_id,
        "expiresAt": expires.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "labels": labels(run.spec.run_id, expires),
        "network": network,
        "record": record,
    }


def connection_secret(run: BenchmarkRun, owner: OwnerRef, password: str, outputs: dict | None) -> dict:
    """Secret `<run>-db`. With `outputs` (the provisioner's connection facts) it carries the `SC_DB_*` variables of the
    benchmark and the bootstrap; without, only the password the up Job reads."""
    data = {"SC_DB_PASSWORD": password}
    if outputs:
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
) -> dict:
    """A Job with one Pod of the provisioner image: no retries inside the Job (the controller decides), no Kubernetes
    API token, the state bucket through the projected web identity token, the vendor's credentials from an optional
    Secret."""
    env_cfg = cfg.env
    vendor_config = cfg.settings.dbaas.vendors.get(vendor)
    job_labels = {LABEL_DBAAS: kind, crd.LABEL_VENDOR: vendor, pods.LABEL_ROLE: PodRole.DBAAS.value}
    if run:
        job_labels[crd.LABEL_RUN] = run
    container_env = [
        {"name": "PULUMI_BACKEND_URL", "value": backend_url(env_cfg)},
        {
            "name": "PULUMI_CONFIG_PASSPHRASE",
            "valueFrom": {"secretKeyRef": {"name": PASSPHRASE_SECRET, "key": "passphrase"}},
        },
        *env,
    ]
    if env_cfg.role_arn:  # the state bucket, as for the controller
        container_env += [
            {"name": "AWS_ROLE_ARN", "value": env_cfg.role_arn},
            {"name": "AWS_WEB_IDENTITY_TOKEN_FILE", "value": f"{AWS_TOKEN_DIR}/token"},
            {"name": "AWS_REGION", "value": env_cfg.bucket_region},
        ]
    metadata = {"name": name, "namespace": cfg.env.namespace, "labels": job_labels}
    if owner:
        metadata["ownerReferences"] = pods._owner_refs(owner)
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": metadata,
        "spec": {
            "backoffLimit": 0,
            "activeDeadlineSeconds": deadline_seconds,
            "template": {
                "metadata": {"labels": {LABEL_JOB: name, pods.LABEL_ROLE: PodRole.DBAAS.value}},
                "spec": {
                    "restartPolicy": "Never",
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
                            "volumeMounts": [
                                {"name": "work", "mountPath": "/work"},
                                {"name": "tmp", "mountPath": "/tmp"},
                                {"name": "aws-token", "mountPath": AWS_TOKEN_DIR, "readOnly": True},
                            ],
                        }
                    ],
                    "volumes": [
                        {"name": "work", "emptyDir": {"sizeLimit": "1Gi"}},
                        {"name": "tmp", "emptyDir": {"sizeLimit": "512Mi"}},
                        {
                            "name": "aws-token",
                            "projected": {
                                "sources": [
                                    {
                                        "serviceAccountToken": {
                                            "audience": "sts.amazonaws.com",
                                            "expirationSeconds": 3600,
                                            "path": "token",
                                        }
                                    }
                                ]
                            },
                        },
                    ],
                },
            },
        },
    }


def up_job(run: BenchmarkRun, cfg: Config, spec: dict, owner: OwnerRef) -> dict:
    """`dbaas-up-<run>`: create the database. The spec is not secret, the password comes from the run's Secret."""
    env = [
        {"name": "DBAAS_SPEC", "value": json.dumps(spec, separators=(",", ":"))},
        {
            "name": "DBAAS_PASSWORD",
            "valueFrom": {"secretKeyRef": {"name": db_secret_name(run.name), "key": "SC_DB_PASSWORD"}},
        },
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


def down_job(run_name: str, vendor: str, cfg: Config, owner: OwnerRef | None) -> dict:
    """`dbaas-down-<run>`: destroy the stack and remove its state. Without an `owner` (a leaked stack the sweeper found)
    the Job belongs to no BenchmarkRun and the sweeper deletes it when it is over."""
    job = _job(
        down_job_name(run_name),
        "down",
        kind="down",
        vendor=vendor,
        cfg=cfg,
        env=[{"name": "DBAAS_RUN", "value": run_name}],
        deadline_seconds=DOWN_DEADLINE_SECONDS,
        run=run_name if owner else None,
        owner=owner,
    )
    if not owner:
        job["metadata"]["labels"][LABEL_SWEEP] = "true"
    return job


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
