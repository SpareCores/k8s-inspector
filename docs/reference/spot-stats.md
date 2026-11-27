# Spot stats

`spot/<vendor>.json` in sc-inspector-data holds spot interruption rates and launch availability per vendor, derived
from the [run records](run-records.md). It is a derived file: the publisher regenerates it from all server run
records of the vendor in the data tree (`data/<vendor>/*/runs/*.json`; DBaaS runs are not included) in the cycle that
writes new run records (and once after a controller start), and commits it (`k8s-inspector: spot stats (<vendors>)`)
only when its content, `generated_at` aside, changed. A vendor without any spot attempt gets no file.

## Schema 1

```json
{"schema": 1, "vendor": "aws", "generated_at": "2026-11-27T13:40:00Z", "method": "...",
 "groups": [
   {"level": "vendor", "key": {}, "observed_from": "2026-11-21", "observed_to": "2026-11-27",
    "waves": [{"from": "2026-11-21", "to": "2026-11-27", "spot_attempts": 412}],
    "spot_attempts": 412, "spot_hours": 905.4, "interruptions": 38,
    "interruptions_per_100h": {"value": 4.2, "ci95": [3.0, 5.8]},
    "launch_tries": 0, "launch_unavailable": 0, "availability": null},
   {"level": "region", "key": {"region": "us-east-1"}, "...": "..."}
 ]}
```

Group levels and keys: `vendor` (`{}`), `region` (`region`), `family` (`family`), `series` (`series`) and
`region_family` (`region`, `family`). Every group has the same fields; `waves` is only on the vendor group.

## Definitions

- A **spot attempt** is an attempt with `capacity_type` `spot` that got a node (it has a `region`).
  `spot_hours` is the sum of their durations (`api_start` to `end`; an open attempt counts 0).
- `interruptions` counts attempts with outcome `SpotInterrupted`. `NodeLost` and every other outcome are not
  interruptions.
- `interruptions_per_100h`: `value` = `interruptions` / `spot_hours` x 100, `ci95` the exact (Garwood) Poisson 95%
  interval of it.
- A **launch try** is a spot attempt that got a node plus an attempt with outcome `SpotUnavailable` (no spot capacity;
  not recorded yet). `launch_unavailable` counts the latter. While no `SpotUnavailable` attempt exists in the
  vendor's records, `launch_tries` and `launch_unavailable` are 0 and `availability` is `null`. Otherwise
  `availability` is (`launch_tries` - `launch_unavailable`) / `launch_tries` with a Wilson 95% interval. An
  unavailable attempt has no region, so it only counts in the vendor, family and series groups.
- `observed_from`, `observed_to`: first and last date (UTC) of the group's attempts.
- `waves` (vendor level): the spot attempts' start dates, split where two consecutive ones are 14 days or more apart.
  Each wave has its first and last date and its spot attempts.
- A group with fewer than 5 spot attempts has the counts only: `interruptions_per_100h` is `null` (as is
  `availability` below 5 launch tries).
- Records without `family` or `series` (written before Keeper's facts were recorded) count in the vendor and region
  groups only.

## Caveats

- **Selection bias**: the runs are benchmarks of the SKUs we chose to run, when we ran them, not a random sample of
  spot usage. Only the regions we ran in appear, and the instance mix is ours.
- Our runs are short and long-running workloads may see other rates; interruptions are not independent (they cluster
  in time), so the Poisson interval is optimistic.
- `NodeLost` is excluded: some of them may be interruptions that were not recognised, so the rate is a lower bound.
- **Validity**: check `observed_to` (stale data says nothing about now), the sample size (`spot_attempts`,
  `spot_hours`) and the `waves`: a group whose attempts all fall into one old wave describes that period only.
