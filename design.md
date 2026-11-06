# MapReduce on Docker Compose — Design Doc

**Team:** Rameez Rahaman (`rrameez99`), Rohan Sohoni
**Course:** CS 544 Honors Project 1, Fall 2026
**Status:** v2 — updated Nov 6, 2026 after design discussion with Tyler (office hours)

---

## 1. Goals

- A working MapReduce system modeled on Dean & Ghemawat (OSDI '04): one **boss** container plus four **worker** containers (each limited to 1 CPU), run with Docker Compose on a single 4-CPU VM.
- A **client** that runs on the host, outside the containers, to upload data and submit jobs.
- **Special features:**
  1. **Combiners**: optional map-side partial reduction, to cut the amount of intermediate data.
  2. **Worker-failure handling**: detect failed or stuck tasks (task timeouts, plus heartbeats) and re-run them so the job still finishes with the correct output.
  3. **Backup tasks for stragglers** (paper §3.6): near the end of a phase, launch duplicate attempts of the remaining in-progress tasks; the first attempt to finish wins. This reuses the same attempt and dedup machinery as feature 2.
- **Stretch goal:** benchmark local-disk shuffle (our design) against a shared-volume shuffle (§3.3).

## 2. Architecture

```
            host
 ┌──────────────────────────┐
 │ mr client (CLI, Python)  │──gRPC──┐
 └────────────┬─────────────┘        │
              │ files                 ▼
        ./shared (bind mount)   ┌──────────┐
              │                 │   boss   │  job/task state, scheduler,
 ─────────────┼─────────────    │          │  failure detector
              ▼                 └────▲─────┘
   /shared in every container        │ heartbeat (+ task assignment)
 ┌─────────┬─────────┬─────────┬─────┴───┐
 │worker-1 │worker-2 │worker-3 │worker-4 │  1 CPU each
 │ /local  │ /local  │ /local  │ /local  │  intermediate files (local disk)
 └────┬────┴────┬────┴────┬────┴────┬────┘
      └──── worker↔worker gRPC: FetchPartition ────┘
```

| Component | Responsibility |
|---|---|
| **Client** | Uploads data to shared storage, submits jobs, polls job status, prints output |
| **Boss** | Keeps job and task state (idle / in-progress / completed, as in paper §3.2), computes input splits, assigns tasks to workers, detects failed workers and re-runs their tasks |
| **Worker** | Runs map and reduce tasks, writes intermediate data to its local disk, serves partitions to reducers, sends heartbeats to the boss |

## 3. Key decisions

### 3.1 Language
**Python 3.12.** It's the course language, and it makes loading user-written `map`/`reduce` functions from a `.py` file simple. The trade-off is lower raw speed, which is acceptable because we compare configurations against each other rather than against Hadoop.

### 3.2 Communication: gRPC + protobufs
- **Client → Boss:** `SubmitJob`, `GetJobStatus`
- **Worker → Boss:** `Register`, `Heartbeat`. Workers **pull** work: the response to each heartbeat may carry a task assignment. We chose pull over push (the boss calling workers) because it's simpler. The boss only runs a gRPC server and never has to track worker addresses to hand out work, and the heartbeat both asks for work and proves the worker is alive.
- **Worker → Boss:** `ReportTaskDone(task_id, status, output locations)`
- **Worker → Worker:** `FetchPartition(map_task_id, reduce_id)` streams one intermediate partition from the mapper's local disk to the reducer.

A sketch of `mapreduce.proto` (to be finalized in the protos PR):
```proto
service Boss {
  rpc SubmitJob(JobSpec) returns (JobId);
  rpc GetJobStatus(JobId) returns (JobStatus);
  rpc Register(WorkerInfo) returns (Ack);
  rpc Heartbeat(HeartbeatReq) returns (HeartbeatResp);   // resp may carry a Task
  rpc ReportTaskDone(TaskResult) returns (Ack);
}
service Worker {
  rpc FetchPartition(PartitionReq) returns (stream Chunk);
}
message JobSpec {
  string input_path = 1;        // dir or file under /shared
  string output_path = 2;       // dir under /shared
  bytes  job_code = 3;          // contents of the user's .py file
  int32  num_map = 4;
  int32  num_reduce = 5;
  bool   use_combiner = 6;
}
```

### 3.3 Shared storage
**Shared bind mount:** `./shared` on the host is mounted at `/shared` in every container. Tyler allows this, it means the client needs no HDFS client, and it keeps the system simple enough to finish both special features.

- Job **input and output** live in `/shared`.
- **Intermediate data** stays on each worker's local disk (`/local`), **not** in `/shared`. This matches the paper. It also means a dead worker's finished map output is lost, so the failure-handling feature has to re-run completed map tasks too (§3.3 of the paper), which makes it a real test.

*Alternatives considered:*
- **Intermediate data on shared storage**, so reducers read map output directly instead of fetching it from mappers. Systems that run MapReduce on serverless functions, with S3 for intermediates, take this approach. It makes worker failures cheaper (finished map output survives) but adds load on shared storage. We'll benchmark it against our design if time allows.
- **HDFS** with a DataNode in each worker container. That would allow locality-aware scheduling, but it adds setup and debugging risk.

### 3.4 Data formats
| Data | Format |
|---|---|
| Input | Line-oriented text files. Map input is `key = (file, byte offset)`, `value = line`, like Hadoop's TextInputFormat |
| Input splits | The boss splits the total input bytes into `num_map` byte ranges, adjusted to line boundaries |
| Intermediate | One directory per map attempt, one file per reduce partition: `/local/job-{id}/map-{m}.{attempt}/part-{r}.jsonl`, with each line a JSON `[key, value]`. The boss records which attempt's location is the accepted one |
| Partitioner | `zlib.crc32(key.encode()) % num_reduce`. **Not** Python's `hash()`, which is randomized per process, so two mappers could send the same key to different reducers |
| Output | `/shared/<output>/part-r-{r:05d}`, with lines of `key\tvalue`, committed as described in §5.3 |

### 3.5 User job interface
The user writes a single `.py` file:
```python
# examples/wordcount.py
def map(key, line):
    for word in line.split():
        yield word.lower(), 1

def combine(key, values):      # optional
    yield key, sum(values)

def reduce(key, values):
    yield key, sum(values)
```
The client sends the file's contents in `SubmitJob`. The boss passes it to each worker with every task, and the worker loads it with `importlib`.

## 4. Client interface

```bash
# 1. Build and start the cluster
docker compose up -d --build

# 2. Install the client deps on the host
pip install -r client/requirements.txt

# 3. Upload example data to shared storage
python -m client upload examples/data/books /input/books

# 4. Submit a job
python -m client submit \
    --input /input/books --output /output/wc \
    --job examples/wordcount.py \
    --maps 8 --reduces 4 [--combiner]

# 5. Check status / view results
python -m client status <job_id>
python -m client cat /output/wc | sort -k2 -nr | head
```

## 5. Fault tolerance and backup tasks (special features 2 and 3)

Every time a task is handed out, it gets a new **attempt ID**. Several attempts of the same task can run at once (after a timeout, or as a backup task), so everything below is built around attempts.

### 5.1 Detecting failures
1. **Task timeout (primary).** If an attempt hasn't finished within a timeout, the boss assigns the task again on another worker. The original attempt keeps running, and whichever finishes first wins. A timeout catches both crashed workers and slow ones (stragglers), which are hard to tell apart. The timeout is a fixed, configurable number of seconds (default **30 s**), set well above a normal task's runtime on our example data.
2. **Heartbeats (optimization).** Workers send a heartbeat every **1 s**. A worker with no heartbeat for **3 s** is marked dead right away, so its tasks are rescheduled without waiting for the full timeout.

### 5.2 What happens when a worker dies
- Its **in-progress** tasks (map or reduce) go back to idle.
- Its **completed map** tasks also go back to idle, because their output was on its local disk (paper §3.3). This only applies while some reduce for the job is still unfinished.
- Completed reduce tasks are kept, since their output is already in `/shared`.
- A reducer whose `FetchPartition` fails reports it to the boss, which re-runs that map task. The reducer then fetches from the new location.

### 5.3 Committing output exactly once
Duplicate attempts (from timeouts or backup tasks) must not produce duplicate or partial output. Each reduce attempt:
1. writes to its **own** temp file: `part-r-{r:05d}.{attempt}.tmp`
2. calls **`fsync`** on the file
3. renames it to the final name with **`os.replace`**, which is atomic
4. calls `fsync` on the output directory

The order matters: the rename makes the switch atomic, but only the `fsync` *before* it guarantees the data is on disk. If two attempts both rename, the final file still holds one complete copy. **No code ever writes directly to a final file name.**

The boss accepts the **first** completion report for each task and ignores later duplicates (it still records them, for the metrics in §8).

### 5.4 Backup tasks
When a phase is nearly done (e.g., ≤ 10% of tasks still in progress), the boss launches one backup attempt for each remaining in-progress task on an idle worker. This is the same path as a timeout reassignment, just triggered earlier.

**Out of scope:** boss failure. We assume a single boss, as the paper does.

## 6. Combiners (special feature 1)

- If `--combiner` is set and the job file defines `combine`, each map task groups its buffered output by key within each partition, applies `combine`, and only then writes the intermediate files.
- The worker records **intermediate bytes written** and **records emitted** for each task and reports them to the boss. That gives us the numbers for the evaluation.

## 7. Testing

| Level | What | Where |
|---|---|---|
| Unit | Split computation (line boundaries, edge cases), user-code loading, combiner gives the same result as without it, task-state transitions on the boss, boss ignores duplicate completions | `pytest`, runs on every PR in GitHub Actions |
| Partitioner stability | Run the partitioner in subprocesses with different `PYTHONHASHSEED` values and check every key lands in the same partition | `pytest` |
| Output commit | Two attempts of the same reduce task race to commit; the final file is complete and appears exactly once, and no `.tmp` files are left in the output | `pytest` |
| Integration | `docker compose up`, run wordcount on the example data, compare with a single-process reference implementation | GitHub Actions (on PRs to `main`) |
| Fault injection | Run a job, `docker compose kill worker-2` mid-job, check the output still matches the reference. Test both the map phase and the reduce phase | Script `tests/test_failures.py`, runs in CI |
| Straggler injection | Slow one worker down (a config flag that adds a delay per record) and check the job finishes correctly, with backup tasks on and off | `tests/test_stragglers.py` |
| Edge cases | `num_reduce = 1`, more maps than lines, empty input file, user code that raises an exception (the task should fail and be retried, without crashing the worker) | `pytest` |

## 8. Performance evaluation

**Dataset:** public-domain Project Gutenberg books, repeated to about 200 MB with `scripts/make_data.py`.
**Method:** each configuration runs 3 times; we report the median with min/max error bars. Results are saved to `eval/results.csv`, and `eval/plots.py` generates the plots.

| # | Plot | Shows |
|---|---|---|
| 1 | **Scaling:** job time vs. number of workers (1–4), line plot | Spreading tasks across workers speeds up jobs (the project's baseline requirement) |
| 2 | **Combiner:** intermediate bytes and job time, with vs. without combiner (wordcount), grouped bars | The combiner shrinks the shuffle and how much time that saves |
| 3 | **Failure timeline:** tasks over time on each worker (Gantt chart), with worker-2 killed at *t = X* | Lost tasks get rescheduled on the survivors, and the job still finishes |
| 4 | **Failure cost:** job time with no failure vs. one worker killed during map vs. during reduce, bars | How much a failure slows the job down |
| 5 | **Backup tasks (like paper Figure 3):** tasks completed over time, for normal / one straggler without backups / one straggler with backups | Backup tasks cut the long tail caused by a slow worker |

The boss logs per-task metrics (start/end time, worker, attempt, bytes written and fetched, re-executions, duplicates ignored), and every plot is built from those logs.

## 9. Repo layout

```
proto/mapreduce.proto
boss/        server.py, scheduler.py, Dockerfile
worker/      server.py, runner.py, Dockerfile
client/      __main__.py, requirements.txt
examples/    wordcount.py, data/
tests/       unit/, integration/, test_failures.py
eval/        run_eval.py, plots.py, results.csv
docker-compose.yml   # boss + worker-1..4, cpus: 1 each, ./shared:/shared
.github/workflows/ci.yml
```

## 10. Division of work

Every PR is reviewed by the other person before it merges, and nobody pushes directly to `main`.

| Area | Owner | Reviewer |
|---|---|---|
| `mapreduce.proto` + generated code | **Both** (pair on it) | — |
| Boss: job/task state, split computation, scheduler | **Rameez** | Rohan |
| Boss: task timeouts, heartbeats, re-execution (**feature 2**) | **Rameez** | Rohan |
| Boss: backup tasks + straggler injection flag (**feature 3**) | **Rameez** | Rohan |
| Worker: attempt-scoped temp files + fsync/`os.replace` commit | **Rohan** | Rameez |
| Worker: map/reduce runner, partitioner, user-code loading | **Rohan** | Rameez |
| Worker: `FetchPartition` server + reducer fetch/retry | **Rohan** | Rameez |
| Combiner + per-task metrics (**feature 1**) | **Rohan** | Rameez |
| Client CLI (`upload`, `submit`, `status`, `cat`) | **Rohan** | Rameez |
| `docker-compose.yml`, Dockerfiles | **Rameez** | Rohan |
| GitHub Actions CI (unit + integration + fault-injection) | **Rameez** | Rohan |
| Unit tests | Each person tests their own code | — |
| Eval: scaling, failure and backup-task plots (1, 3, 4, 5) | **Rameez** | Rohan |
| Eval: combiner plot (2), dataset script | **Rohan** | Rameez |
| README.md | **Both** | — |

**Design doc sections:** Rameez owns §2, §3.1–3.3, §5, §7. Rohan owns §3.4–3.5, §4, §6, §8.

## 11. Timeline

| Dates | Milestone | PRs |
|---|---|---|
| Nov 2–6 | Design doc merged | `design-skeleton` (Rameez), `design-worker-client` (Rohan) |
| Nov 7–9 | Protos, Docker Compose, CI skeleton, stub boss/worker that register and heartbeat | 3–4 small PRs |
| Nov 10–12 | End-to-end wordcount working (no special features) | boss scheduler, worker runner, client |
| Nov 13–14 | Combiner + failure handling, fault-injection tests | 2–3 PRs |
| Nov 15 | Eval runs + plots | `eval-*` |
| Nov 16 | README polish, everything merged to `main` | final PR |
| Nov 17–20 | Demo with Tyler | — |

## 12. Code conventions

- **Errors are never silently ignored.** Each one is handled (e.g., retried), propagated (raised to the caller), or ends the process with a clear message.
- **No placeholder defaults to keep going.** Missing or malformed critical input (a bad job file, a missing input path, a corrupt partition) fails loudly instead of being filled with zeros or skipped.
- **No speculative defensive code.** We know our own classes and protobuf messages, so no `getattr(obj, "x", None)` and no trying several field names "just in case."
- **Names say what things are:** `map_attempt_id`, `accepted_location`, not `data2` or `result_obj`.
- **Every PR gets a real review**, including any AI-assisted code, which is read critically like any other code before it merges.