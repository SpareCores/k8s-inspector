"""k8s-inspector controller: plans, runs and publishes Spare Cores benchmark runs on Kubernetes.

Design: one process (`k8s-inspector serve`) with plain threads, not asyncio. lightkube's sync client has a
blocking `watch` generator and boto3/git are blocking anyway, so threads map one-to-one onto the work:
two watch threads (BenchmarkRuns, labelled Pods) and a timer feed one queue; ONE worker thread reconciles
runs from that queue (no locks inside the state machine); a planner thread and a publisher thread run their
periodic loops. They share only `Kube`, `Storage` (both thread-safe) and write BenchmarkRun status through
`crd.update_status` (optimistic concurrency).

Layout: model, config, log, kube, crd, tasks, steps, storage,
pods, spot, runner, planner, keeper, variant, render, publisher, cli.
"""
