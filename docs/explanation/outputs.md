# Output pipeline

Goal: every byte a benchmark writes reaches its **final** object storage key, the benchmark is never slowed down by
the upload, nothing on the node holds a credential, and a node is released only after its uploads are confirmed.
There is no inbox and no archive move: the key an output is uploaded to is the key it keeps
([layout](../reference/output-contract.md#object-storage)).

## On the node: `sc-run`

`sc-run` is the wrapper in every step container (Go 1.27.4, static, standard library plus embedded CA roots; it needs no AWS SDK because the
controller signs every request). It:

1. runs the version command (60 s cap), then the task command through the image's resource-tracker entrypoint;
2. writes stdout and stderr to files in the **spool**, a hostPath directory on the node's boot disk
   (`/var/lib/k8s-inspector/runs/<run_id>/<task>/`), directly as the task's stdout and stderr (no pipes, so a background process that keeps them open cannot fail the task; a poller copies the appended bytes to sc-run's own stdout and stderr about once a second and once more after the task exits, so `kubectl logs` shows the output; `--mirror-logs=false` turns that off; the spool files and the uploaded objects stay the source of truth; the mirror is best-effort: it skips ahead, with a marker line, when it falls more than 1 MiB behind, and it never delays the task);
3. enforces the task timeout (SIGTERM, SIGKILL after 5 s), forwards signals, reaps children, runs the task at nice -20 (`--nice`, -20 to 19, else harness error 256; the applied value or null goes to `result.json`), sets rlimits for the DB
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

Retries: exponential backoff per request. `--upload-budget` (10 min) is a hard bound on the whole final upload,
`manifest.json` included: the file uploads get the first three quarters, the manifest the rest. A file that cannot be
uploaded stays in the spool and is listed as missing in the manifest. Each distinct retry error of an object is logged
at once; a repeated one only every few attempts.

`sc-run` embeds the root CA certificates in the binary (`golang.org/x/crypto/x509roots/fallback`): the benchmark images carry no CA bundle (some have no
`/etc/ssl` at all) and the `sc-run` image is `FROM scratch`, so HTTPS to object storage would fail with `unknown
authority` otherwise.

Measured on OCI (Frankfurt) against S3 in us-east-1:

| Mechanism | Result |
|---|---|
| Growing log re-PUT | objects replaced about 60 s apart; `resource_tracker.jsonl` grows about 220 KB/min (13 MB/h), so it passes 32 MiB after about 2.4 h and only the 26 h `stressnglongrun` needs multipart for it |
| Multipart part retry | a part with a bad signature returned 403 `SignatureDoesNotMatch` and was retried with backoff; after the corrected Secret was applied, the part succeeded within 100 s (the kubelet mount refresh) and `ListParts` showed all 4 parts |
| URL re-sign during a step | URLs expired after 600 s; the first re-PUT after expiry got 403. After the re-signed Secret was applied the next re-PUT succeeded within 4 s; the gap was 2 minutes with no data loss, because whole files are re-PUT |
| Flush | a step with missing URLs ended `upload_complete: false, missing: 5`; after the Secret was restored, a `flush` step (started 6 s after the update) uploaded everything and `verify` passed |

## In the controller

The controller is the only component with object storage credentials (cluster OIDC → cloud IAM role, decision K4).
Its credentials refresh `resignEveryMinutes` + 15 minutes before they expire, so a presigned URL never outlives the
credentials that signed it.

| When | What |
|---|---|
| Before creating a step | Starts a multipart upload (`CreateMultipartUpload`) for each large-capable file, presigns the PUT URLs and the UploadPart URLs (16 MiB parts: 16 for `stdout` and `stderr`, `uploads.maxParts` for `resource_tracker.jsonl` and the task's `large_outputs`), writes them into the run's Secret, which the step mounts. Only serial tasks have large-capable standard files; the parallel hardware probes run without a resource tracker and upload every file with a single PUT. The URL file stays below 700 KiB (a Secret holds 1 MiB): a step that exceeds it is not started and its tasks end `256`. |
| Every hour while a step runs | Re-signs the URLs (signatures live at most 12 h, the role session limit). The kubelet refreshes the mounted Secret within about a minute; `sc-run` re-reads it before each request. A 26-hour task therefore uploads directly. |
| When a step ends | Reads `manifest.json`, completes each multipart upload with the reported ETags (`CompleteMultipartUpload`), checks every object's size with `HEAD`, aborts unused multipart uploads, then writes the task's `status.json` next to the outputs. **`status.json` is the task's commit point.** |
| If anything is missing | Creates a `flush` step on the same node (the `sc-run` image, fresh URLs, same spool) and checks again. After two failed flushes the controller completes what the uploaded parts allow (salvage) and records the task with `outputs lost` when it had exited 0. |
| Only then | Moves on to the next step, or releases the node by deleting the anchor. |

Before every launch of a task (a retry in the same run writes the same keys) the controller deletes the task's
`manifest.json`, `result.json` and `status.json`, so `verify` only sees the manifest of the current attempt. The
controller's bucket role therefore needs `s3:DeleteObject` under the run prefix.

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

For the node-loss case the controller first deletes the step Pods and waits until none can still upload (while the
node exists), then salvages: it completes the in-flight multipart uploads from `ListParts`, keeps the last re-PUT of the
small logs, and records the task as `-4` with the reason. Previous tasks of that run are untouched, because each was
confirmed before the next began. Nothing is salvaged when the task runs again (a spot interruption with another
attempt left) or when the run is cancelled: the open uploads are aborted, as nobody reads the partial output.

A bucket lifecycle rule `AbortIncompleteMultipartUpload` (7 days) on the output prefix removes parts of uploads that
were never completed. The rule on `logs/` does not cover keys under `K8SI_KEY_PREFIX`: a prefix needs its own rule.

## Publishing to git

The publisher runs in the controller every 5 minutes and on demand. It never moves data.

1. **What to publish** comes from Kubernetes: tasks in `BenchmarkRun.status.tasks` with `state: Done` and
   `published: false`. (After a cluster loss: the rescan in [state model](state.md#disaster-recovery).)
2. `git fetch` + `reset --hard origin/main` in a blobless, sparse, shallow checkout.
3. For each task, read `status.json` and the outputs from their final keys and render them into the tree exactly as
   sc-inspector does: variant dir, `meta.json`, `stdout`/`stderr` (the `raw` transform), geekbench `results.json`,
   the parsers (`dmidecode`, `openssl`, `stressnglongrun` → `parsed.*`, `lstopo` → `lstopo.svg`), timing files, the
   `schedule/` union. A task whose `start` is older than the meta already in git is skipped (stale duplicate).
   - The **variant** is picked from the `lscpu` output of the attempt that ran the task (`run.json`
     `attempts[].lscpu_stdout`, an attempt being the one in the task's `status.json`) and the
     `variant.json` files in the checkout; the publisher assigns a new number when the hardware is new. A task with
     a negative exit code and no usable `lscpu` result goes to `schedule/<task>/meta.json` only; one with exit code
     0 or above waits for the next cycle rather than land in the wrong variant, until `keepRunHours` after its end,
     unless the run's `lscpu` task failed (or was rejected) and every task of the run is done: no `lscpu` result will
     come, so a failed task without an `lscpu` result of its attempt goes to `schedule/<task>/meta.json` only (the
     planner then plans by its exit code: `-4` is retried after its wait, `256` is final), and a successful one,
     whose hardware is unknown, is given up at once instead of being filed under a guessed variant. Tasks of an earlier
     attempt that did have an `lscpu` result still go to their variant. A warning is logged for the run, and its SKU
     waits for the launch backoff (reason `NO_LSCPU`, [launch retries](launch-retries.md)) before the planner starts
     it again. A task that is given up is marked published without a commit, logged as an error and counted in
     `k8s_inspector_unpublishable_tasks_total`, so the run no longer holds the SKU lease. The run is deleted `keepRunHours` after that, so such a run holds
the lease about twice `keepRunHours` after its end.
   - The **timing** files come from the attempt of the task: the latest `run.json` `attempts[]` entry with an lscpu
     result (else its `timing` map) gives `api_start` (the attempt's anchor created), `api_end`, `machine_start`,
     `node_registered` and `node_ready`; the publisher adds `inspector_start`/`inspector_end` (first task start, last
     task end). They go to the variant of that attempt.
   - **A failed task never replaces a good result.** When the tree already holds an exit code 0 result of the
     task in the variant, a result with another exit code (a failure, a cancelled task's partial output, an
     out-of-memory kill) is not rendered: its files, `meta.json` and parsed output stay as they were, and the failure
     stays in the run status. If the task hash changed since the good result, the failure goes to
     `schedule/<task>/meta.json` (picked among the variants like any meta, so a current-hash success of another variant
     still wins) and the planner waits before it retries; if the new hash keeps failing, the old-hash result stays in the
     tree. A failure still replaces an earlier failure,
     and a success replaces anything.
   - **Transforms and parsers run only for tasks with exit code 0**; `stdout` and `stderr` are written for every
     exit code except the harness error `256`.
   - **`-5`** is created here, not by the runner: the runner records `-4`, and the publisher, which sees the
     previous `meta.json` in git, counts consecutive `-4` of the same task hash in `start_retries` and writes the
     third as `-5` ([exit codes](../reference/exit-codes.md)). Any other exit code, or a changed hash, resets the
     count.
   - A SKU is published only when its vendor is enabled with `publish: true` or a list that names it
     ([configuration](../reference/configuration.md)); the runs of other SKUs are deleted after `keepRunHours`, their
     outputs stay in object storage, and `publish --rescan --sku` can publish them later. While the ConfigMap is
     missing or empty the defaults are in effect, which allow nothing; the controller then deletes no unpublished
     run.
   - A forced run of a SKU that git does not know and that has no `lscpu` result (a typo) publishes nothing.
   - A task whose rendering fails 12 cycles in a row is published without parser output.
   - Git over SSH pins GitHub's published host keys (`StrictHostKeyChecking=yes`); the deploy key is the only secret.
4. One commit per cycle, push. On a non-fast-forward, start again at step 2: rendering is a pure function of object
   storage plus the tree, so there is no rebase or conflict handling.
5. After the push, set `published: true` (with the commit SHA) on those tasks. A crash between push and patch
   re-renders the same content, finds no diff and patches.

When all tasks of a run are published, the controller deletes the `BenchmarkRun` after 24 h.
`resource_tracker.jsonl` and `postgres.log` stay in object storage only (not in git, as today).

## Why a custom uploader

No mainstream tool consumes presigned multipart URLs with part-level retry and a disk spool: curl can do a single
presigned PUT, aws-cli, rclone and s5cmd need credentials, Fluent Bit's presigned mode is new, thinly documented and a
much bigger agent. A wrapper is needed anyway to enforce timeouts, keep stdout and stderr apart and set rlimits, so the
uploader is ~300 lines inside it. Go gives one static binary that runs in every image (scratch, Ubuntu, postgres,
CUDA), with a few MB of memory: a part is streamed from disk, not buffered.

Rejected alternatives (presigned POST policies, `kubectl logs`, `exec tar`, cluster tokens on benchmark Pods,
temporary STS keys on the node): [alternatives](alternatives.md#outputs).
