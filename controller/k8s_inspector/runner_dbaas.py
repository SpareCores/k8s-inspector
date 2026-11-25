"""The database of a DBaaS run, as part of the run state machine (`Runner` mixes this in).

The order is: client node first, database second; on the way out, database destroy and node release in parallel
(decision K11, docs/explanation/dbaas.md). `status.database.phase` is the state:

    (none) -> Creating -> Bootstrapping -> Ready -> Destroying -> Destroyed

The phase is written BEFORE what it stands for is created, so a controller that restarts anywhere knows there may be a
database to destroy and creates the missing Job again. Creating and Bootstrapping are steps of the run's `Running`
phase (`_database_up`); Destroying is driven on every pass of every run phase (`_database_teardown`) as soon as the
benchmark is over, however it ended. The destroy Job is retried until it succeeds; only then is the finalizer removed.
A stack left by an earlier run of the same name is destroyed before the run asks for its client node (`Clearing` in
`Pending`, then back to `(none)`); if that destroy fails `CLEAR_ATTEMPTS` times the run ends with `-1`
(`DBAAS_LEFTOVER_STACK`), holding no database, so the usual retry cap stops it, and the stack is left to the sweeper.

A running `dbaas-up` Job is never deleted: it ends within its own deadline (Pulumi records what it created) and only
then the destroy starts. Deleting it would risk a database the state never recorded.

The Jobs have no Kubernetes API access: they report through their Pod's termination message, which this module reads.
"""

import base64
import logging
import secrets
from datetime import datetime, timedelta

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

ERROR_CHARS = 2000  # of a Job's error in `status.database.error`: the provisioner condenses it to fit (`__main__`)
TIMEOUT_MARGIN = timedelta(minutes=5)  # the controller judges the create deadline a little after the Job's own
POLL_SECONDS = 60  # while waiting for a Job: the Job and Pod watches wake the run, this bounds the wait for a deadline
WAIT_FOR_POD_SECONDS = 10  # the up Job's Pod is still going away; the down Job must not start before it is gone
RETRY_MIN_SECONDS, RETRY_MAX_SECONDS = 60, 1800  # between destroy attempts: doubles from a minute to half an hour
ENDED_PHASES = ("Succeeded", "Failed")  # any other Pod phase (even Unknown) may still run the Pulumi process
STUCK_POD_MARGIN = timedelta(minutes=10)  # a deleted Pod this long past its grace period is on a lost node
CLEAR_ATTEMPTS = 3  # failed destroys of a leftover stack after which the run ends -1 and leaves it to the sweeper
RUNNING_PHASES = (RunPhase.PENDING, RunPhase.PROVISIONING, RunPhase.RUNNING)


def _new_password() -> str:
    """28 characters (vendors cap at 30 or 32), with a letter of each case, a digit and a symbol, which the password
    policies of the vendors ask for; the only symbol is `-`, and the rest are letters and digits."""
    return f"Sc1-{secrets.token_hex(12)}"


def database_over(run: BenchmarkRun) -> bool:
    """The benchmark is over and the database may go: the run is releasing or finished in any way, cancelled or
    deleted. Between attempts of a running run (`Pending`) the database stays."""
    return run.cancel_requested or run.deleting or run.status.phase not in RUNNING_PHASES


def pod_ended(pod: dict) -> bool:
    return (pod.get("status") or {}).get("phase") in ENDED_PHASES


class DbaasMixin:
    # ----------------------------------------------------------------------- creating

    def _database_ready(self, run: BenchmarkRun) -> bool:
        return run.status.database is not None and run.status.database.phase == DbPhase.READY

    def _dbaas_task(self, run: BenchmarkRun) -> TaskDef:
        return self._selected(run)[0]

    def _set_database(self, run: BenchmarkRun, mutate) -> BenchmarkRun:
        """Update `status.database` (the run may have left `Running` meanwhile: the database does not care)."""

        return crd.update_status(self.kube, run.name, mutate)

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

    def _database_fail(self, run: BenchmarkRun, reason: str, message: str) -> None:
        """The database cannot be used: the run's task ends `-1` (retried by the usual rules). The destroy follows from
        the run leaving `Running`."""
        text = f"{reason}: {message}"[:ERROR_CHARS]
        log.warning("run %s: %s", run.name, text)

        def mutate(status: RunStatus) -> None:
            if status.database is not None:
                status.database.error = text

        run = self._set_database(run, mutate)
        self._end_run(run, ExitCode.NO_NODE, text, AttemptOutcome.PROVISIONING_FAILED)

    def _database_start(self, run: BenchmarkRun, cfg: Config, live: dict[str, dict]) -> None:
        db = run.spec.database
        vendor_config = cfg.settings.dbaas.vendors.get(db["vendor"])
        if not cfg.env.dbaas_image:
            return self._end_run(run, ExitCode.HARNESS_ERROR, "K8SI_DBAAS_IMAGE is empty", AttemptOutcome.HARNESS_ERROR)
        client_region = pods.client_region(db)
        if not (vendor_config and client_region in vendor_config.networks):
            return self._database_fail(
                run, "DBAAS_NO_NETWORK", f"no network of {db['vendor']} in {client_region} in the ConfigMap"
            )
        now = self._now()
        expires = dbaas_jobs.expiry(now, cfg, self._dbaas_task(run))
        run = self._set_database(
            run,
            lambda status: setattr(
                status, "database", DbStatus(DbPhase.CREATING, now, expires, timing={"api_start": now})
            ),
        )
        self._database_creating(run, cfg, live)

    def _clearing(self, run: BenchmarkRun) -> bool:
        """The run waits for the destroy of a stack an earlier run left: it has created no database."""
        return run.status.database is not None and run.status.database.phase == DbPhase.CLEARING

    def _clear_leftover_stack(self, run: BenchmarkRun, cfg: Config) -> BenchmarkRun | None:
        """Called from `Pending`, before the run asks for a client node: destroy a stack left by an earlier run of the
        same name (`Clearing`). Returns the run to go on with, or None while the destroy is going on (or the run
        ended). A destroy that fails `CLEAR_ATTEMPTS` times (pending operations, say) ends the run with `-1`
        `DBAAS_LEFTOVER_STACK`: it holds no database, the retry cap stops the planner, the stack stays for the sweeper
        and the operator, and a paid client node never waits for it."""
        db = run.status.database
        if db is not None and db.phase != DbPhase.CLEARING:
            return run  # an earlier attempt of this run made its database
        state_key = dbaas_jobs.stack_state_key(run.name)
        if db is None:
            if not any(key == state_key for key, _ in self.storage.list_objects(state_key)):
                return run
            if not cfg.env.dbaas_image:
                self._end_run(run, ExitCode.HARNESS_ERROR, "K8SI_DBAAS_IMAGE is empty", AttemptOutcome.HARNESS_ERROR)
                return None
            log.warning("run %s: a Pulumi stack of this name exists, destroying it first", run.name)
            now = self._now()
            run = self._set_database(
                run, lambda status: setattr(status, "database", DbStatus(DbPhase.CLEARING, now, now))
            )
            db = run.status.database
        done, run = self._drive_destroy(run, cfg, db)
        if done:
            return self._set_database(run, lambda status: setattr(status, "database", None))
        db = run.status.database
        if db.destroy_failures >= CLEAR_ATTEMPTS:
            reason = db.error.removeprefix("DBAAS_DESTROY_FAILED: ") if db.error else "unknown"
            stack = dbaas_jobs.stack_name(run.name)
            text = f"DBAAS_LEFTOVER_STACK: {stack} could not be destroyed ({reason}); operator action needed"
            if self._on_leftover_stack_failure:
                self._on_leftover_stack_failure(run.spec.vendor)
            run = self._set_database(run, lambda status: setattr(status, "database", None))
            self._end_run(run, ExitCode.NO_NODE, text[:500], AttemptOutcome.PROVISIONING_FAILED)
        return None

    def _password(self, run: BenchmarkRun) -> str:
        """The run's own database password: generated once, kept in the run's Secret (so it survives a restart)."""
        name = pods.db_secret_name(run.name)
        try:
            data = self.kube.get("secret", name).get("data") or {}
            return base64.b64decode(data[dbaas_jobs.RUN_PASSWORD_KEY]).decode()
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
        network = cfg.settings.dbaas.vendors[db["vendor"]].networks[pods.client_region(db)]
        nd = self._nodes[run.name]
        node = self._get_node(nd.facts.node_name) or {}
        addresses = dbaas_jobs.client_addresses(node, nd.claim)
        spec = dbaas_jobs.build_spec(run, cfg, record, network, run.status.database.expires, addresses)
        self._create("job", dbaas_jobs.up_job(run, cfg, spec, self._run_owner(run)))

    def _database_creating(self, run: BenchmarkRun, cfg: Config, live: dict[str, dict]) -> None:
        db = run.status.database
        name = dbaas_jobs.up_job_name(run.name)
        try:
            job = self.kube.get("job", name)
        except NotFound:
            return self._create_up_job(run, cfg)
        state = dbaas_jobs.job_state(job)
        if state == "failed":
            message = dbaas_jobs.job_message(self.kube, name).get("error") or "the Job failed"
            return self._database_fail(run, "DBAAS_CREATE_FAILED", message)
        if state == "running":
            if self._now() - db.since > timedelta(minutes=cfg.settings.dbaas.create_minutes) + TIMEOUT_MARGIN:
                return self._database_fail(
                    run, "DBAAS_CREATE_TIMEOUT", f"not created within {cfg.settings.dbaas.create_minutes} minutes"
                )
            return self._enqueue_later(run.name, POLL_SECONDS)
        outputs = dbaas_jobs.job_message(self.kube, name)
        if "host" not in outputs:
            return self._database_fail(run, "DBAAS_CREATE_FAILED", "the Job reported no connection facts")
        self.kube.apply_secret(dbaas_jobs.connection_secret(run, self._run_owner(run), self._password(run), outputs))
        now = self._now()
        created = dbaas_jobs.job_completed_at(job, now)

        def mutate(status: RunStatus) -> None:
            status.database.phase, status.database.since = DbPhase.BOOTSTRAPPING, now
            status.database.timing["api_end"] = created

        run = self._set_database(run, mutate)
        self.kube.delete("job", name)  # a vendor-generated password is in its Pod's termination message
        self._start_bootstrap(run, cfg, live)

    # ----------------------------------------------------------------------- bootstrapping

    def _start_bootstrap(self, run: BenchmarkRun, cfg: Config, live: dict[str, dict]) -> None:
        self.kube.delete("job", dbaas_jobs.up_job_name(run.name))  # again: the controller may have restarted before
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
        if phase == "Failed" or (phase == "Succeeded" and "error" in dbaas_jobs.termination_message(pod)):
            message = (
                dbaas_jobs.termination_message(pod).get("error") or pod["status"].get("reason") or "the Pod failed"
            )
            return self._database_fail(run, "DBAAS_BOOTSTRAP_FAILED", message)
        if phase != "Succeeded":
            return self._enqueue_later(run.name, POLL_SECONDS)
        self.kube.delete("pod", name)
        now = self._now()

        def mutate(status: RunStatus) -> None:
            status.database.phase, status.database.since = DbPhase.READY, now

        run = self._set_database(run, mutate)
        self._running(run, cfg, {k: v for k, v in live.items() if k != name})

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
                status.database.timing["api_destroy_start"] = now

            run = self._set_database(run, mutate)
            db = run.status.database
        self._destroy(run, cfg, db)

    def _destroy(self, run: BenchmarkRun, cfg: Config, db: DbStatus) -> None:
        destroyed_at, run = self._drive_destroy(run, cfg, db)
        if not destroyed_at:
            return
        self.kube.delete("secret", pods.db_secret_name(run.name))

        def mutate(status: RunStatus) -> None:
            status.database.phase = DbPhase.DESTROYED
            status.database.timing["api_destroy_end"] = destroyed_at

        self._set_database(run, mutate)
        crd.remove_finalizer(self.kube, run.name)
        log.info("run %s: the database is destroyed", run.name)

    def _drive_destroy(self, run: BenchmarkRun, cfg: Config, db: DbStatus) -> tuple[datetime | None, BenchmarkRun]:
        """One pass of the destroy of the run's stack: when it succeeded and its state is gone, the time the destroy
        Job completed (else None), with the run as it is now (a failed destroy updated it). A running up Job is
        waited for, never deleted: it ends within its deadline and Pulumi records what it created."""
        up = dbaas_jobs.up_job_name(run.name)
        try:
            up_job = self.kube.get("job", up)
        except NotFound:
            up_job = None
        keep_up = up_job is not None and dbaas_jobs.job_state(up_job) == "failed"  # its logs stay for `kubectl logs`
        if up_job is not None and not keep_up:
            if dbaas_jobs.job_state(up_job) == "running":
                self._enqueue_later(run.name, POLL_SECONDS)
                return None, run
            self.kube.delete("job", up)
        pods_left = self.kube.list("pod", labels={dbaas_jobs.LABEL_JOB: up})
        if keep_up:  # the kept Pod stays: only one that may still run Pulumi (even in its grace period) is waited for
            pods_left = [pod for pod in pods_left if not pod_ended(pod)]
        stuck = [pod for pod in pods_left if self._pod_stuck(pod)]
        pods_left = [pod for pod in pods_left if pod not in stuck]
        if pods_left:  # the destroy starts when the up Pulumi process cannot run any more
            self._enqueue_later(run.name, WAIT_FOR_POD_SECONDS)
            return None, run
        if stuck:  # a Pod past its grace on a lost node runs no Pulumi any more: the database must not bill on
            pod_name = stuck[0]["metadata"]["name"]
            text = (
                f"DBAAS_UP_POD_STUCK: Pod {pod_name} is terminating long past its grace period, proceeding with destroy"
            )
            log.error("run %s: %s", run.name, text)
            run = self._set_database(run, lambda status: setattr(status.database, "error", text))
        name = dbaas_jobs.down_job_name(run.name)
        try:
            job = self.kube.get("job", name)
        except NotFound:
            job = None
        if job is not None and dbaas_jobs.job_state(job) == "failed":
            if not dbaas_jobs.failure_counted(job, db):
                return None, self._destroy_failed(run, job, db)
            if db.retry_after and self._now() < db.retry_after:  # the failed Job stays until its retry replaces it
                self._enqueue_later(run.name, max(1.0, (db.retry_after - self._now()).total_seconds()))
                return None, run
            self.kube.delete("job", name)
            job = None
        if job is None:
            if db.retry_after and self._now() < db.retry_after:
                self._enqueue_later(run.name, max(1.0, (db.retry_after - self._now()).total_seconds()))
                return None, run
            self._create("job", dbaas_jobs.down_job(run.name, run.spec.vendor, cfg, failures=db.destroy_failures))
            self._enqueue_later(run.name, POLL_SECONDS)
            return None, run
        state = dbaas_jobs.job_state(job)
        if state == "running":
            self._enqueue_later(run.name, POLL_SECONDS)
            return None, run
        dbaas_jobs.clean_state(self.storage, run.name)
        self.kube.delete("job", name)
        return dbaas_jobs.job_completed_at(job, self._now()), run

    def _pod_stuck(self, pod: dict) -> bool:
        """Deleted, and now is past its `deletionTimestamp` plus its grace period plus `STUCK_POD_MARGIN`."""
        deleted = pod["metadata"].get("deletionTimestamp")
        if not deleted:
            return False
        grace = pod.get("spec", {}).get("terminationGracePeriodSeconds", pods.DEFAULT_GRACE_SECONDS)
        return self._now() > datetime.fromisoformat(deleted) + timedelta(seconds=grace) + STUCK_POD_MARGIN

    def _destroy_failed(self, run: BenchmarkRun, job: dict, db: DbStatus) -> BenchmarkRun:
        """The destroy failed: try again later, as long as it takes (a database that bills must not stay). A Job
        without the failure annotation is from before it existed: it is deleted here so it is counted once."""
        name = job["metadata"]["name"]
        message = dbaas_jobs.job_message(self.kube, name).get("error") or "the destroy Job failed"
        failures = db.destroy_failures + 1
        wait = min(RETRY_MIN_SECONDS * 2 ** (failures - 1), RETRY_MAX_SECONDS)
        retry_after = self._now() + timedelta(seconds=wait)
        log.error("run %s: destroying the database failed (%d), retrying in %ds", run.name, failures, wait)
        if dbaas_jobs.ANNOTATION_DESTROY_FAILURES not in (job["metadata"].get("annotations") or {}):
            self.kube.delete("job", name)

        def mutate(status: RunStatus) -> None:
            status.database.destroy_failures, status.database.retry_after = failures, retry_after
            status.database.error = f"DBAAS_DESTROY_FAILED: {message}"[:ERROR_CHARS]

        run = self._set_database(run, mutate)
        self._enqueue_later(run.name, wait)
        return run
