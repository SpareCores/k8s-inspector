"""DBaaS planning: which database gets a BenchmarkRun, in which region and with which client (autoPlan and forced).

A DBaaS run is a BenchmarkRun with a `database` block; it is named after the database. Its client node is any small
x86_64 type (`dbaas_client`) that the cluster launches next to the database, in the cheapest region of the database that
the cluster platform has a network for; only the price bound comes from Keeper's server data. Databases,
versions,
regions, prices and eligibility are Keeper's (`dbaas_keeper`, fail closed). What is due comes from the
`dbaas/<vendor>/<id>/postgres/<version>/standalone/<task>/meta.json` files in git with the same rules as servers
(`planner.due_reason`: a changed hash, `-1` and `-4` retried with their cap, then `-5`). The DBaaS runs of a vendor have
their own limits, `dbaasMaxInFlight` and `dbaasMaxUsdPerHour`, besides the vendor's server runs. See
docs/explanation/dbaas.md.
"""

import logging
import threading
from collections import Counter
from collections.abc import Callable, Collection
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from k8s_inspector import crd, dbaas_client, dbaas_keeper, launch, planner, pods, publisher, tasks
from k8s_inspector.kube import Kube
from k8s_inspector.model import BenchmarkRun, CapacityMode, Config, DbPhase, ExitCode, RunSpec, TaskDef

log = logging.getLogger(__name__)

# `{database: {engine version: {task: meta}}}` of a vendor (`Publisher.read_dbaas_metas`)
DbaasReader = Callable[[str], dict[str, dict[str, dict[str, dict | None]]]]


@dataclass(frozen=True)
class PlannedDatabase:
    vendor: str
    database_id: str
    engine_version: str
    region: str  # the database's region
    client: str | None  # the operator's choice of client SKU; None: any small x86_64 type
    tasks: tuple[str, ...]
    reason: str  # for logs and `plan --dry-run`
    client_region: str | None = None  # where the client runs when it is not `region`


def dbaas_tasks() -> list[TaskDef]:
    return [task for task in tasks.all_tasks() if task.dbaas]


def holds_database(run: BenchmarkRun) -> bool:
    """A DBaaS run that holds or is acquiring a client node or a database: active, or not yet destroyed."""
    if run.spec.database is None:
        return False
    destroyed = run.status.database is not None and run.status.database.phase == DbPhase.DESTROYED
    return run.status.phase in planner.ACTIVE_PHASES or (run.status.database is not None and not destroyed)


@dataclass(frozen=True)
class Placement:
    region: str  # the database's region
    client_region: str  # the client node's region: `region`, or one of its city
    client: str | None  # the operator's choice of client SKU; None: any small x86_64 type
    usd_per_hour: float | None  # database + client node (Keeper's on-demand prices, the dearest type); None: unknown


def database_run(runs: list[BenchmarkRun], vendor: str, database_id: str) -> BenchmarkRun | None:
    """The run of the database, whatever its name: a run made before runs were named after their database is named
    after its client SKU."""
    return next((r for r in runs if r.spec.vendor == vendor and (r.spec.database or {}).get("id") == database_id), None)


def holding_runs(runs: list[BenchmarkRun], vendor: str) -> list[BenchmarkRun]:
    return [run for run in runs if run.spec.vendor == vendor and holds_database(run)]


def check_vendor(cfg: Config, vendor: str, runs: list[BenchmarkRun]) -> None:
    """Raise `ValueError` unless one more DBaaS run of `vendor` may start: the vendor is enabled, has networks
    configured, and is below its `dbaasMaxInFlight` (0 pauses; server runs have their own limit)."""
    vendor_settings = cfg.settings.vendors.get(vendor)
    if vendor_settings is None or not vendor_settings.enabled:
        raise ValueError(f"vendor {vendor} is not enabled in the ConfigMap")
    vendor_config = cfg.settings.dbaas.vendors.get(vendor)
    if vendor_config is None or not vendor_config.networks:
        raise ValueError(f"no dbaas.vendors.{vendor}.networks in the ConfigMap: the platform provides no DBaaS network")
    limit = vendor_settings.dbaas_max_in_flight
    running = [run.name for run in holding_runs(runs, vendor)]
    if len(running) >= limit:
        raise ValueError(
            f"vendor {vendor} is at its dbaasMaxInFlight limit ({len(running)} of {limit} DBaaS runs in flight"
            f"{': ' + ', '.join(running) if running else ''})"
        )


def check_spend(cfg: Config, vendor: str, runs: list[BenchmarkRun], price: float | None) -> None:
    """Raise `ValueError` when `dbaasMaxUsdPerHour` is set and the DBaaS runs of `vendor` that hold a database or client
    node, plus a new run at `price`, would cost more. Prices are the ones recorded in the runs (`database.usdPerHour`);
    a run or candidate without a price makes the spend unknown, which is refused."""
    cap = cfg.settings.vendors[vendor].dbaas_max_usd_per_hour
    if cap is None:
        return
    if price is None:
        raise ValueError(
            f"vendor {vendor} has dbaasMaxUsdPerHour {cap}: the price of the database or client is unknown"
        )
    holding = holding_runs(runs, vendor)
    unpriced = [run.name for run in holding if run.spec.database.get("usdPerHour") is None]
    if unpriced:
        raise ValueError(f"vendor {vendor} has dbaasMaxUsdPerHour {cap}: the price of {', '.join(unpriced)} is unknown")
    spend = sum(run.spec.database["usdPerHour"] for run in holding)
    if spend + price > cap:
        raise ValueError(
            f"vendor {vendor} would pass its dbaasMaxUsdPerHour cap ({cap} USD/hour): {spend:.4f} running + {price:.4f}"
        )


def region_order(prices: dict[str, float], networks: dict, failed: Collection[str] = ()) -> list[str]:
    """The database's regions that have a network, cheapest first; the regions where the failed attempts in a row
    were go last, so a retry tries another one."""
    return sorted((r for r in prices if r in networks), key=lambda r: (r in failed, prices[r], r))


def client_regions(
    region: str, networks: dict, cities: dict[str, tuple[str, str]], *, by_city: bool = False
) -> list[str]:
    """The regions the client of a database in `region` may run in, in the order they are tried: the region itself when
    the platform has a network there; otherwise, only for a vendor with `clientRegionByCity` (OVH: databases and servers
    are in differently named regions), the regions of the same Keeper city (country and city) that have one."""
    if region in networks:
        return [region]
    if not by_city:
        return []
    city = cities.get(region)
    return [r for r in sorted(networks) if city and cities.get(r) == city]


def _failed_regions(meta: dict | None) -> set[str]:
    """The regions of the attempts that failed without a result in a row: the published `failed_regions` (start
    failures counted for the retry cap), else the last attempt's region when it failed without one."""
    attempt = meta.get("last_attempt") or meta if meta else {}
    regions = set(attempt.get("failed_regions") or [])
    if attempt.get("region") and attempt.get("exit_code") in (ExitCode.NO_NODE, ExitCode.NEVER_FINISHED):
        regions.add(attempt["region"])
    return regions


def _create_run(
    kube: Kube,
    cfg: Config,
    vendor: str,
    record: dict,
    version: str,
    placement: Placement,
    names: list[str],
    *,
    force: bool,
    hold_after: timedelta = timedelta(0),
) -> BenchmarkRun:
    spec = RunSpec(
        vendor=vendor,
        sku=record["api_reference"],
        run_id=crd.new_run_id(datetime.now(UTC)),
        tasks=names,
        force=force,
        capacity=CapacityMode.ON_DEMAND,  # a lost client node would leave a database billing with nobody to use it
        hold_after=hold_after,
        database={
            "vendor": vendor,
            "id": record["api_reference"],
            "engineVersion": version,
            "region": placement.region,
            **({"client": placement.client} if placement.client else {}),
            **({"clientRegion": placement.client_region} if placement.client_region != placement.region else {}),
            "vcpus": int(record["vcpus"]),
            "memoryGib": float(record["memory_amount"]) / 1024,
        },
    )
    if placement.usd_per_hour is not None:
        spec.database["usdPerHour"] = placement.usd_per_hour
    run = crd.create_run(kube, spec)
    log.info("created DBaaS run", extra={"run": run.name, "vendor": vendor, "database": record["api_reference"]})
    return run


def _resolve(
    cfg: Config,
    vendor: str,
    record: dict,
    *,
    region: str | None,
    client: str | None,
    failed_regions: Collection[str],
    client_region: str | None = None,
) -> Placement:
    """The given database region, client region and client, else the cheapest database region that has a client region
    (its own or one of its city, with a network) and a client type in Keeper's data there, with the hourly price of
    database and client (the dearest type that may be launched) from Keeper. Raise `ValueError` when there is none."""
    vendor_config = cfg.settings.dbaas.vendors[vendor]
    networks = vendor_config.networks
    database_id = record["api_reference"]
    prices = dbaas_keeper.standalone_prices(cfg.env, vendor, database_id)
    cities = dbaas_keeper.region_cities(cfg.env, vendor)

    def clients_of(database_region: str) -> list[str]:
        return (
            [client_region]
            if client_region
            else client_regions(database_region, networks, cities, by_city=vendor_config.client_region_by_city)
        )

    if client_region and client_region not in networks:
        raise ValueError(f"no DBaaS network of {vendor} in {client_region} in the ConfigMap")
    if region:
        if not clients_of(region):
            where = f"{region} or in a region of its city" if vendor_config.client_region_by_city else region
            raise ValueError(f"no DBaaS network of {vendor} in {where} in the ConfigMap")
        regions = [region]
    else:
        reachable = {r: price for r, price in prices.items() if clients_of(r)}
        # a run records the region of its node, which is the client's
        failed = {r for r in reachable if failed_regions and set(failed_regions) & {r, *clients_of(r)}}
        regions = region_order(reachable, reachable, failed)
    if not regions:
        raise ValueError(f"{vendor}/{database_id} has no standalone price in a region that has a DBaaS network")
    ids = dbaas_keeper.region_ids(cfg.env, vendor)
    for candidate in regions:
        for client_at in clients_of(candidate):
            if client_at not in ids:
                continue
            servers = dbaas_keeper.client_servers(cfg.env, vendor, ids[client_at])
            if client or dbaas_client.most_expensive(servers) is not None:
                return Placement(candidate, client_at, client, _hourly(prices.get(candidate), servers, client))
    raise ValueError(f"no client server for {vendor}/{database_id} in {', '.join(regions)}")


def _hourly(database_price: float | None, servers: list[dict], client: str | None) -> float | None:
    """Database price plus the client's `min_price_ondemand` in Keeper's server data: the named client's, else the
    dearest type that may be launched; None when either is missing."""
    if client:
        client_price = next((s.get("min_price_ondemand") for s in servers if s.get("api_reference") == client), None)
    else:
        client_price = dbaas_client.most_expensive(servers)
    if database_price is None or client_price is None:
        return None
    return float(database_price) + float(client_price)


def force_run(
    kube: Kube,
    cfg: Config,
    vendor: str,
    database_id: str,
    *,
    region: str | None = None,
    client_region: str | None = None,
    client: str | None = None,
    force: bool = True,
    hold_after: timedelta = timedelta(0),
) -> BenchmarkRun:
    """`k8s-inspector run <vendor> --database <id>`: a DBaaS run of one database now. Raise `ValueError` for validation
    errors and `crd.RunExists` when the database already has a run. Without `force` Keeper must find the database
    eligible (unreachable Keeper: refused); with it only Keeper's record must exist. `region` is the database's;
    `region`, `client_region` and `client` default to the cheapest database region with a client region (its own or
    one of its city, with a network) and any small x86_64 client type."""
    if client and not pods.is_label_value(client):
        raise ValueError(f"{client} is not a valid label value: SCarpenter cannot select it by api-reference")
    runs = crd.list_runs(kube)
    check_vendor(cfg, vendor, runs)
    if existing := database_run(runs, vendor, database_id):
        raise crd.RunExists(existing.name)
    try:
        record = dbaas_keeper.database(cfg.env, vendor, database_id)
    except RuntimeError as e:
        raise ValueError(f"cannot read the database from Keeper: {e}") from e
    if record is None:
        raise ValueError(f"Keeper does not know the database {vendor}/{database_id}")
    names = [task.name for task in dbaas_tasks()]
    if not force:
        try:
            blocked = dbaas_keeper.database_block_reasons(cfg.env, vendor, database_id, names)
        except RuntimeError as e:
            raise ValueError(f"cannot check eligibility with Keeper: {e} (use --force to skip the check)") from e
        if blocked:
            details = "; ".join(f"{name}: {', '.join(reasons)}" for name, reasons in sorted(blocked.items()))
            raise ValueError(
                f"Keeper blocks tasks for {vendor}/{database_id} ({details}); use --force to skip the check"
            )
    version = dbaas_keeper.engine_version(record, cfg.settings.dbaas.engine_version)
    if version is None:
        raise ValueError(f"{vendor}/{database_id} has no PostgreSQL version in Keeper")
    try:
        placement = _resolve(
            cfg,
            vendor,
            record,
            region=region,
            client=client,
            failed_regions=(),
            client_region=client_region,
        )
    except RuntimeError as e:
        raise ValueError(f"cannot read regions or servers from Keeper: {e}") from e
    check_spend(cfg, vendor, crd.list_runs(kube), placement.usd_per_hour)
    return _create_run(kube, cfg, vendor, record, version, placement, names, force=force, hold_after=hold_after)


def plan_cycle(
    kube: Kube,
    cfg: Config,
    reader: DbaasReader,
    *,
    dry_run: bool = False,
    keeper_cache: dbaas_keeper.Cache | None = None,
    now: datetime | None = None,
) -> list[PlannedDatabase]:
    """One DBaaS autoPlan cycle. For each enabled vendor with `dbaasAutoPlan` whose results are published, that has no
    DBaaS run in flight: take its databases Keeper lists as eligible, in `planner.start_order`, and create a run for the
    first one with a due task (`planner.due_reason` on the git metas) that has a region with a network and a client.
    Fails closed: when Keeper or git cannot be read nothing is planned for that cycle or vendor. With `dry_run` nothing
    is created and the first database of each vendor is returned."""
    names = [task.name for task in dbaas_tasks()]
    cache = keeper_cache or dbaas_keeper.Cache()
    vendors = [
        vendor
        for vendor, settings in cfg.settings.vendors.items()
        if settings.enabled and settings.dbaas_auto_plan and publisher.vendor_allowed(cfg.settings, vendor)
    ]
    if not vendors:
        return []
    try:
        blocked = cache.block_reasons(cfg.env, names)
    except Exception:
        log.exception("DBaaS autoPlan skips the cycle: Keeper is not reachable")
        return []
    now = (now or datetime.now(UTC)).replace(tzinfo=None)
    planned = []
    for vendor in vendors:
        try:
            check_vendor(cfg, vendor, crd.list_runs(kube))
            if launch.paused(kube, vendor, datetime.now(UTC)):
                raise ValueError("its nodes do not come up")
        except ValueError as e:
            log.info("DBaaS autoPlan skips vendor %s: %s", vendor, e)
            continue
        try:
            records = {r["api_reference"]: r for r in cache.databases(cfg.env, vendor)}
            metas = reader(vendor)
        except Exception:
            log.exception("DBaaS autoPlan skips vendor %s: cannot read Keeper or the sc-inspector-data state", vendor)
            continue
        runs = crd.list_runs(kube)
        skipped: Counter[str] = Counter()  # why the databases got no run, for the line logged when none did
        block_codes: Counter[str] = Counter()
        for database_id in sorted(blocked.get(vendor, {}), key=lambda d: planner.start_order(vendor, d)):
            record = records.get(database_id)
            if database_run(runs, vendor, database_id):
                skipped["already has a run"] += 1
                continue
            if record is None:
                skipped["no Keeper record"] += 1
                continue
            if not publisher.publish_allowed(cfg.settings, vendor, database_id):
                skipped["publish not allowed"] += 1
                continue
            version = dbaas_keeper.engine_version(record, cfg.settings.dbaas.engine_version)
            if version is None:
                skipped["no PostgreSQL version"] += 1
                continue
            if all(name in blocked[vendor][database_id] for name in names):
                skipped["blocked by Keeper"] += 1
                block_codes.update(code for codes in blocked[vendor][database_id].values() for code in codes)
                continue
            due, failed_regions = {}, set()
            for task in dbaas_tasks():
                if task.name in blocked[vendor][database_id]:
                    continue
                meta = metas.get(database_id, {}).get(version, {}).get(task.name)
                if meta is None and task.name in metas.get(database_id, {}).get(version, {}):
                    break  # damaged meta.json: skip the database
                try:
                    reason = planner.due_reason(task, meta, now)
                except AttributeError, TypeError, ValueError:
                    log.warning("DBaaS autoPlan skips %s/%s: damaged meta.json of %s", vendor, database_id, task.name)
                    break
                if reason:
                    due[task.name] = reason
                    failed_regions |= _failed_regions(meta)
            if not due:
                skipped["nothing due"] += 1
                continue
            try:
                placement = _resolve(cfg, vendor, record, region=None, client=None, failed_regions=failed_regions)
                check_spend(cfg, vendor, crd.list_runs(kube), placement.usd_per_hour)
            except (ValueError, RuntimeError) as e:
                log.info("DBaaS autoPlan skips %s/%s: %s", vendor, database_id, e)
                skipped["no region, client or budget"] += 1
                continue
            if not dry_run:
                try:
                    _create_run(kube, cfg, vendor, record, version, placement, list(due), force=False)
                except crd.RunExists:
                    continue
            reason = ", ".join(f"{name}: {why}" for name, why in due.items())
            planned.append(
                PlannedDatabase(
                    vendor,
                    database_id,
                    version,
                    placement.region,
                    placement.client,
                    tuple(due),
                    reason,
                    placement.client_region,
                )
            )
            break  # at most one new DBaaS run per vendor and cycle; the next cycle checks the limits again
        else:
            log.info(_nothing_planned(vendor, len(blocked.get(vendor, {})), skipped, block_codes))
    return planned


def _nothing_planned(vendor: str, total: int, skipped: Counter[str], block_codes: Counter[str]) -> str:
    """The line of a vendor that got no run in a cycle: the databases Keeper lists and how many were skipped why."""
    parts = []
    for reason, count in skipped.items():
        example = f" (e.g. {block_codes.most_common(1)[0][0]})" if reason == "blocked by Keeper" and block_codes else ""
        parts.append(f"{count} {reason}{example}")
    return f"DBaaS autoPlan: nothing to plan for vendor {vendor}: {total} databases" + "".join(f", {p}" for p in parts)


def run_forever(
    kube: Kube, cfg_provider, reader: DbaasReader, stop: threading.Event, keeper_cache: dbaas_keeper.Cache | None = None
) -> None:
    """Every `planner.interval_minutes`: `plan_cycle`; exceptions are logged, never fatal."""
    cache = keeper_cache or dbaas_keeper.Cache()
    while not stop.is_set():
        interval_minutes = 5
        try:
            cfg = cfg_provider()
            interval_minutes = cfg.settings.planner_interval_minutes
            for planned in plan_cycle(kube, cfg, reader, keeper_cache=cache):
                log.info(
                    "planned DBaaS run",
                    extra={"vendor": planned.vendor, "database": planned.database_id, "reason": planned.reason},
                )
        except Exception:
            log.exception("DBaaS planning cycle failed")
        stop.wait(interval_minutes * 60)
