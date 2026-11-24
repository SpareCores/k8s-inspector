"""The database of a DBaaS run, as part of the run state machine (`Runner` mixes this in).

The order is: client node first, database second; on the way out, database destroy and node release in parallel
(decision K11, docs/explanation/dbaas.md). `status.database.phase` is the state:

    (none) -> Creating -> Bootstrapping -> Ready -> Destroying -> Destroyed

The phase is written BEFORE what it stands for is created, so a controller that restarts anywhere knows there may be a
database to destroy and creates the missing Job again. Creating and Bootstrapping are steps of the run's `Running`
phase (`_database_up`); Destroying is driven on every pass of every run phase (`_database_teardown`) as soon as the
benchmark is over, however it ended. The destroy Job is retried until it succeeds; only then is the finalizer removed.

The Jobs have no Kubernetes API access: they report through their Pod's termination message, which this module reads.
"""

import base64
import json
import logging
import secrets
from datetime import timedelta

from k8s_inspector import crd, dbaas_jobs, dbaas_keeper, pods
from k8s_inspector.kube import NotFound
from k8s_inspector.model import (
    AttemptOutcome,
    BenchmarkRun,
    Config,
    DbPhase,
    DbStatus,
    ExitCode,
    RunPhase,
    RunStatus,
    TaskDef,
)

log = logging.getLogger(__name__)

TIMEOUT_MARGIN = timedelta(minutes=5)  # the controller judges the create deadline a little after the Job's own
POLL_SECONDS = 60  # while waiting for a Job: the Job and Pod watches wake the run, this bounds the wait for a deadline
WAIT_FOR_POD_SECONDS = 10  # the up Job's Pod is still going away; the down Job must not start before it is gone
RETRY_MIN_SECONDS, RETRY_MAX_SECONDS = 60, 1800  # between destroy attempts: doubles from a minute to half an hour
RUNNING_PHASES = (RunPhase.PENDING, RunPhase.PROVISIONING, RunPhase.RUNNING)


def job_state(job: dict) -> str:
    """`complete`, `failed` or `running`."""
    for condition in job.get("status", {}).get("conditions") or []:
        if condition.get("status") == "True" and condition.get("type") in ("Complete", "Failed"):
            return condition["type"].lower()
    return "running"


def _new_password() -> str:
    """Random, with a letter of each case, a digit and a symbol, which the password policies of the vendors ask for."""
    return f"Sc1-{secrets.token_urlsafe(24)}"


def database_over(run: BenchmarkRun) -> bool:
    """The benchmark is over and the database may go: the run is releasing or finished in any way, cancelled or
    deleted. Between attempts of a running run (`Pending`) the database stays."""
    return run.cancel_requested or run.deleting or run.status.phase not in RUNNING_PHASES


class DbaasMixin:
    # ----------------------------------------------------------------------- creating

    def _database_ready(self, run: BenchmarkRun) -> bool:
        return run.status.database is not None and run.status.database.phase == DbPhase.READY

    def _dbaas_task(self, run: BenchmarkRun) -> TaskDef:
        return self._selected(run)[0]

    def _set_database(self, run: BenchmarkRun, mutate) -> BenchmarkRun:
        """Update `status.database` (the run may have left `Running` meanwhile: the database does not care)."""

        def apply(status: RunStatus) -> None:
            mutate(status)

        return crd.update_status(self.kube, run.name, apply)

    def _database_up(self, run: BenchmarkRun, cfg: Config, live: dict[str, dict]) -> None:
        """One pass of Creating and Bootstrapping, from `_running` while the database is not Ready. The anchor's hold is
        kept alive meanwhile: nothing else touches it for up to an hour."""
        self._heartbeat(run)
        db = run.status.database
        if db is None:
            return self._database_start(run, cfg, live)
        if db.phase == DbPhase.CREATING:
            return self._database_creating(run, cfg, live)
        if db.phase == DbPhase.BOOTSTRAPPING:
            return self._database_bootstrapping(run, cfg, live)
        self._database_fail(run, "DBAAS_GONE", "the database is already being destroyed")

    def _database_fail(self, run: BenchmarkRun, reason: str, message: str, *, destroy: bool = False) -> None:
        """The database cannot be used: the run's task ends `-1` (retried by the usual rules). The destroy follows from
        the run leaving `Running`; with `destroy` (a database may exist although none was created) the phase is set
        first."""
        text = f"{reason}: {message}"[:500]
        log.warning("run %s: %s", run.name, text)
        now = self._now()

        def mutate(status: RunStatus) -> None:
            if status.database is None and destroy:
                status.database = DbStatus(DbPhase.CREATING, now, now)
            if status.database is not None:
                status.database.error = text

        run = self._set_database(run, mutate)
        self._end_run(run, ExitCode.NO_NODE, text, AttemptOutcome.PROVISIONING_FAILED)

    def _database_start(self, run: BenchmarkRun, cfg: Config, live: dict[str, dict]) -> None:
        db = run.spec.database
        vendor_config = cfg.settings.dbaas.vendors.get(db["vendor"])
        if not cfg.env.dbaas_image:
            return self._end_run(run, ExitCode.HARNESS_ERROR, "K8SI_DBAAS_IMAGE is empty", AttemptOutcome.HARNESS_ERROR)
        if not (vendor_config and db["region"] in vendor_config.networks):
            return self._database_fail(
                run, "DBAAS_NO_NETWORK", f"no network of {db['vendor']} in {db['region']} in the ConfigMap"
            )
        state_key = dbaas_jobs.stack_state_key(run.name)
        if any(key == state_key for key, _ in self.storage.list_objects(state_key)):
            # A stack of this name is left from an earlier run: destroy it before anything new is created.
            return self._database_fail(run, "DBAAS_STACK_EXISTS", "a Pulumi stack of this run exists", destroy=True)
        now = self._now()
        expires = dbaas_jobs.expiry(now, cfg, self._dbaas_task(run))
        run = self._set_database(
            run, lambda status: setattr(status, "database", DbStatus(DbPhase.CREATING, now, expires))
        )
        self._database_creating(run, cfg, live)

    def _password(self, run: BenchmarkRun) -> str:
        """The run's database password: generated once, kept in the run's Secret (so it survives a restart)."""
        name = pods.db_secret_name(run.name)
        try:
            data = self.kube.get("secret", name).get("data") or {}
            return base64.b64decode(data["SC_DB_PASSWORD"]).decode()
        except NotFound, KeyError:
            password = _new_password()
            self.kube.apply_secret(dbaas_jobs.connection_secret(run, self._run_owner(run), password, None))
            return password

    def _create_up_job(self, run: BenchmarkRun, cfg: Config) -> None:
        db = run.spec.database
        record = dbaas_keeper.database(cfg.env, db["vendor"], db["id"])
        if record is None:
            return self._database_fail(run, "DBAAS_UNKNOWN_DATABASE", f"Keeper does not know {db['vendor']}/{db['id']}")
        self._password(run)
        network = cfg.settings.dbaas.vendors[db["vendor"]].networks[db["region"]]
        spec = dbaas_jobs.build_spec(run, record, network, run.status.database.expires)
        self._create("job", dbaas_jobs.up_job(run, cfg, spec, self._run_owner(run)))

    def _job_message(self, job_name: str) -> dict:
        """The JSON in the termination message of the Job's Pod; empty when there is none."""
        for pod in self.kube.list("pod", labels={dbaas_jobs.LABEL_JOB: job_name}):
            for status in pod.get("status", {}).get("containerStatuses") or []:
                text = status.get("state", {}).get("terminated", {}).get("message")
                if text:
                    try:
                        message = json.loads(text)
                    except ValueError:
                        return {"error": text[:500]}
                    return message if isinstance(message, dict) else {}
        return {}

    def _database_creating(self, run: BenchmarkRun, cfg: Config, live: dict[str, dict]) -> None:
        db = run.status.database
        name = dbaas_jobs.up_job_name(run.name)
        try:
            job = self.kube.get("job", name)
        except NotFound:
            return self._create_up_job(run, cfg)
        state = job_state(job)
        if state == "failed":
            message = self._job_message(name).get("error") or "the Job failed"
            return self._database_fail(run, "DBAAS_CREATE_FAILED", message)
        if state == "running":
            if self._now() - db.since > timedelta(minutes=cfg.settings.dbaas.create_minutes) + TIMEOUT_MARGIN:
                return self._database_fail(
                    run, "DBAAS_CREATE_TIMEOUT", f"not created within {cfg.settings.dbaas.create_minutes} minutes"
                )
            return self._enqueue_later(run.name, POLL_SECONDS)
        outputs = self._job_message(name)
        if "host" not in outputs:
            return self._database_fail(run, "DBAAS_CREATE_FAILED", "the Job reported no connection facts")
        self.kube.apply_secret(dbaas_jobs.connection_secret(run, self._run_owner(run), self._password(run), outputs))
        now = self._now()

        def mutate(status: RunStatus) -> None:
            status.database.phase, status.database.since = DbPhase.BOOTSTRAPPING, now

        run = self._set_database(run, mutate)
        self._start_bootstrap(run, cfg, live)

    # ----------------------------------------------------------------------- bootstrapping

    def _start_bootstrap(self, run: BenchmarkRun, cfg: Config, live: dict[str, dict]) -> None:
        nd = self._nodes[run.name]
        owner = self._anchor_owner(run, live)
        self._create("configmap", dbaas_jobs.boot_configmap(run, self._run_owner(run)))
        self._create(
            "pod",
            dbaas_jobs.boot_pod(
                run, run.status.attempt, cfg, self._dbaas_task(run), node_name=nd.facts.node_name, owner=owner
            ),
        )
        self._enqueue_later(run.name, POLL_SECONDS)

    def _database_bootstrapping(self, run: BenchmarkRun, cfg: Config, live: dict[str, dict]) -> None:
        name = dbaas_jobs.boot_pod_name(run.name, run.status.attempt)
        pod = live.get(name)
        if pod is None:
            return self._start_bootstrap(run, cfg, live)  # first time, or the Pod was lost with an earlier attempt
        phase = pod.get("status", {}).get("phase")
        if phase == "Failed" or (phase == "Succeeded" and "error" in self._pod_message(pod)):
            message = self._pod_message(pod).get("error") or pod["status"].get("reason") or "the Pod failed"
            return self._database_fail(run, "DBAAS_BOOTSTRAP_FAILED", message)
        if phase != "Succeeded":
            return self._enqueue_later(run.name, POLL_SECONDS)
        self.kube.delete("pod", name)
        now = self._now()

        def mutate(status: RunStatus) -> None:
            status.database.phase, status.database.since = DbPhase.READY, now

        run = self._set_database(run, mutate)
        self._running(run, cfg, {k: v for k, v in live.items() if k != name})

    @staticmethod
    def _pod_message(pod: dict) -> dict:
        for status in pod.get("status", {}).get("containerStatuses") or []:
            text = status.get("state", {}).get("terminated", {}).get("message")
            if text:
                try:
                    message = json.loads(text)
                except ValueError:
                    return {"error": text[:500]}
                return message if isinstance(message, dict) else {}
        return {}

    # ----------------------------------------------------------------------- destroying

    def _database_teardown(self, run: BenchmarkRun, cfg: Config) -> None:
        """Every pass of a DBaaS run, in any phase: start the destroy when the benchmark is over, drive the down Job
        until it succeeds, then remove the finalizer."""
        db = run.status.database
        if db is None:  # nothing was ever created
            if run.deleting:
                crd.remove_finalizer(self.kube, run.name)
            return
        if db.phase == DbPhase.DESTROYED:
            if run.deleting:
                crd.remove_finalizer(self.kube, run.name)
            return
        if db.phase != DbPhase.DESTROYING:
            if not database_over(run):
                return
            now = self._now()

            def mutate(status: RunStatus) -> None:
                status.database.phase, status.database.since = DbPhase.DESTROYING, now

            run = self._set_database(run, mutate)
            db = run.status.database
        self._destroy(run, cfg, db)

    def _destroy(self, run: BenchmarkRun, cfg: Config, db: DbStatus) -> None:
        up = dbaas_jobs.up_job_name(run.name)
        self.kube.delete("job", up)  # stops a creation that is still going; a finished Job is just removed
        if self.kube.list("pod", labels={dbaas_jobs.LABEL_JOB: up}):  # it must be gone before the destroy starts
            return self._enqueue_later(run.name, WAIT_FOR_POD_SECONDS)
        name = dbaas_jobs.down_job_name(run.name)
        try:
            job = self.kube.get("job", name)
        except NotFound:
            if db.retry_after and self._now() < db.retry_after:
                return self._enqueue_later(run.name, max(1.0, (db.retry_after - self._now()).total_seconds()))
            owner = self._run_owner(run)
            self._create("job", dbaas_jobs.down_job(run.name, run.spec.vendor, cfg, owner))
            return self._enqueue_later(run.name, POLL_SECONDS)
        state = job_state(job)
        if state == "running":
            return self._enqueue_later(run.name, POLL_SECONDS)
        if state == "failed":
            return self._destroy_failed(run, name, db)
        self._destroyed(run, name)

    def _destroy_failed(self, run: BenchmarkRun, name: str, db: DbStatus) -> None:
        """The destroy failed: try again later, as long as it takes (a database that bills must not stay)."""
        message = self._job_message(name).get("error") or "the destroy Job failed"
        failures = db.destroy_failures + 1
        wait = min(RETRY_MIN_SECONDS * 2 ** (failures - 1), RETRY_MAX_SECONDS)
        retry_after = self._now() + timedelta(seconds=wait)
        log.error("run %s: destroying the database failed (%d), retrying in %ds", run.name, failures, wait)

        def mutate(status: RunStatus) -> None:
            status.database.destroy_failures, status.database.retry_after = failures, retry_after
            status.database.error = f"DBAAS_DESTROY_FAILED: {message}"[:500]

        self._set_database(run, mutate)
        self.kube.delete("job", name)
        self._enqueue_later(run.name, wait)

    def _destroyed(self, run: BenchmarkRun, job_name: str) -> None:
        """The destroy succeeded: what is left of the stack in the bucket goes, then the connection facts, and the run
        may be deleted. Every step can be repeated."""
        dbaas_jobs.clean_state(self.storage, run.name)
        self.kube.delete("secret", pods.db_secret_name(run.name))
        self._set_database(run, lambda status: setattr(status.database, "phase", DbPhase.DESTROYED))
        crd.remove_finalizer(self.kube, run.name)
        self.kube.delete("job", job_name)
        log.info("run %s: the database is destroyed", run.name)
