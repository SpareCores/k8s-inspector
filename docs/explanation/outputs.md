# Output pipeline

Goal: every byte a benchmark writes reaches its **final** object storage key, the benchmark is never slowed down by
the upload, nothing on the node holds a credential, and a node is released only after its uploads are confirmed.
There is no inbox and no archive move: the key an output is uploaded to is the key it keeps
([layout](../reference/output-contract.md#object-storage)).

## On the node: `sc-run`

`sc-run` is the wrapper in every step container (Go, static, standard library only; it needs no AWS SDK because the
controller signs every request). It:

1. runs the version command (60 s cap), then the task command through the image's resource-tracker entrypoint;
2. writes stdout and stderr to files in the **spool**, a hostPath directory on the node's boot disk
   (`/var/lib/k8s-inspector/runs/<run_id>/<task>/`), directly as the task's stdout and stderr (no pipes, so a background process that keeps them open cannot fail the task; the container log stays empty, read the spool or the uploaded objects);
3. enforces the task timeout (SIGTERM, SIGKILL after 5 s), forwards signals, reaps children, sets rlimits for the DB
   tasks, touches a heartbeat file for the anchor;
4. uploads in a separate goroutine, so the task never waits on the network:
   - **growing logs** (stdout, stderr, `resource_tracker.jsonl`): re-PUT the whole file every 60 s when it changed,
     to a reusable presigned PUT URL (one URL per key, valid until expiry; each PUT atomically replaces the object);
   - **large files** (above 32 MiB, or any file the task definition marks large): S3 multipart. Each 16 MiB part is
     uploaded once it is complete (logs only grow), with per-part retry; the tail is uploaded as the current last
     part number and overwritten as it grows (re-uploading a part number replaces it);
   - **final flush** when the task exits: every file once more, then `manifest.json` (file names, sizes, md5,
     multipart part ETags; [format](../reference/objects.md#manifestjson)). The manifest is written last; its presence means "sc-run finished uploading".
5. writes a short summary (exit code, timed out, OOM kills, upload state) to the termination message and exits 0; the
   task's own exit code is in the result, never in the Pod phase.

Retries: exponential backoff per request, bounded by the step's upload budget (10 min). A file that cannot be uploaded
stays in the spool and is listed as missing in the manifest.

## In the controller

The controller is the only component with object storage credentials (cluster OIDC → cloud IAM role, decision K4).

| When | What |
|---|---|
| Before creating a step | Starts a multipart upload (`CreateMultipartUpload`) for each large-capable file, presigns the PUT URLs and the UploadPart URLs (16 MiB × 64 parts per large file, up to 1 GiB), writes them into the run's Secret, which the step mounts. |
| Every hour while a step runs | Re-signs the URLs (signatures live at most 12 h, the role session limit). The kubelet refreshes the mounted Secret within about a minute; `sc-run` re-reads it before each request. A 26-hour task therefore uploads directly. |
| When a step ends | Reads `manifest.json`, completes each multipart upload with the reported ETags (`CompleteMultipartUpload`), checks every object's size with `HEAD`, aborts unused multipart uploads, then writes the task's `status.json` next to the outputs. **`status.json` is the task's commit point.** |
| If anything is missing | Creates a `flush` step on the same node (the `sc-run` image, fresh URLs, same spool) and checks again. After two failed flushes the task is recorded with `outputs lost`. |
| Only then | Moves on to the next step, or releases the node by deleting the anchor. |

Presigning of `CreateMultipartUpload`, `CompleteMultipartUpload` and `AbortMultipartUpload` is technically possible but
not something AWS documents; the controller calls them itself, and only `PutObject` and `UploadPart` are presigned.

## What can still be lost

| Event | Loss | Why |
|---|---|---|
| Task crash, OOM, timeout | nothing | `sc-run` survives the task and flushes |
| `sc-run` killed with the container (step deadline) | at most 60 s of log tail | the spool survives; the `flush` step uploads it |
| Upload endpoint down for a while | nothing | spool + retries + `flush` |
| Controller down | nothing for finished steps; uploads continue with URLs valid up to 12 h | the anchor idle timeout releases the node 45 min after the last heartbeat |
| Spot reclaim, `expireAfter`, manual node delete | at most the in-flight task (≤60 s of its log tail is in storage) | `do-not-disrupt` stops only voluntary disruption; notices are 30 s-5 min ([vendors](../reference/vendors.md)) |

For the node-loss case the controller salvages what it can: it completes the in-flight multipart uploads from
`ListParts` (normally only `sc-run`'s ETags are used), keeps the last re-PUT of the small logs, and records the task as
`-4` with the reason. Previous tasks of that run are untouched, because each was confirmed before the next began.

A bucket lifecycle rule `AbortIncompleteMultipartUpload` (7 days) on the output prefix removes parts of uploads that
were never completed.

## Publishing to git

The publisher runs in the controller every 5 minutes and on demand. It never moves data.

1. **What to publish** comes from Kubernetes: tasks in `BenchmarkRun.status.tasks` with `state: Done` and
   `published: false`. (After a cluster loss: the rescan in [state model](state.md#disaster-recovery).)
2. `git fetch` + `reset --hard origin/main` in a blobless, sparse, shallow checkout.
3. For each task, read `status.json` and the outputs from their final keys and render them into the tree exactly as
   sc-inspector does: variant dir, `meta.json`, `stdout`/`stderr` (the `raw` transform), geekbench `results.json`,
   the parsers (`dmidecode`, `openssl`, `stressnglongrun` → `parsed.*`, `lstopo` → `lstopo.svg`), timing files, the
   `schedule/` union. A task whose `start` is older than the meta already in git is skipped (stale duplicate).
4. One commit per cycle, push. On a non-fast-forward, start again at step 2: rendering is a pure function of object
   storage plus the tree, so there is no rebase or conflict handling.
5. After the push, set `published: true` (with the commit SHA) on those tasks. A crash between push and patch
   re-renders the same content, finds no diff and patches.

When all tasks of a run are published, the controller deletes the `BenchmarkRun` after 24 h.
`resource_tracker.jsonl` and `postgres.log` stay in object storage only (git-ignored, as today).

## Why a custom uploader

No mainstream tool consumes presigned multipart URLs with part-level retry and a disk spool: curl can do a single
presigned PUT, aws-cli, rclone and s5cmd need credentials, Fluent Bit's presigned mode is new, thinly documented and a
much bigger agent. A wrapper is needed anyway to enforce timeouts, keep stdout and stderr apart and set rlimits, so the
uploader is ~300 lines inside it. Go gives one static binary that runs in every image (scratch, Ubuntu, postgres,
CUDA), with a few MB of memory: a part is streamed from disk, not buffered.

Rejected alternatives (presigned POST policies, `kubectl logs`, `exec tar`, cluster tokens on benchmark Pods,
temporary STS keys on the node): [alternatives](alternatives.md#outputs).
