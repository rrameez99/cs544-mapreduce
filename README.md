# cs544-mapreduce

MapReduce implementation running on Docker Compose (boss + 4 workers). CS 544 Honors Project, UW-Madison, Fall 2026

## Host client

The client runs on the host and stages input in `./shared`, which Docker
Compose mounts as `/shared` in every container. On a clean checkout, install
the client dependencies and generate the ignored protobuf stubs first:

```bash
pip install -r client/requirements.txt
python scripts/gen_protos.py
```

Start the cluster from the repository root:

```bash
docker compose up -d --build
```

Then use the client commands below. Client paths are relative to the shared
namespace, so `/input/books` becomes `/shared/input/books` in the RPC sent to
the boss.

```bash
# Copy a host file or directory into ./shared/input/books.
python -m client upload path/to/books /input/books

# Submit a job. The boss creates and reserves /shared/output/wc.
python -m client submit \
  --input /input/books --output /output/wc \
  --job path/to/wordcount.py \
  --maps 8 --reduces 4 --combiner --backup-tasks

# Poll once per second until the job finishes, then print the final reducer files.
python -m client status job-1 --wait
python -m client cat /output/wc | sort -k2 -nr | head
```

`cat` reads only final `part-r-*` files. It deliberately excludes the
per-job `_events.jsonl` evaluation log and any temporary files.

## Combiner evaluation

The design's combiner comparison uses the included word-count job and an
offline, deterministic Project Gutenberg dataset generator. Download one or
more public-domain UTF-8 text files yourself, then build the approximately
200 MiB dataset directly in the host directory that Compose mounts as
`/shared/input/books`:

```bash
pip install -r eval/requirements.txt
python scripts/gen_protos.py
python scripts/make_data.py path/to/gutenberg-book-1.txt path/to/gutenberg-book-2.txt
docker compose up -d --build
```

The generator preserves complete source files and their UTF-8 characters. Its
default output is `shared/input/books/books.txt`, so it is already available
to the cluster as the client path `/input/books`; no separate upload is
needed. Use `--target-bytes` to choose a different lower-bound size or
`--output` to use a different new host directory.

Run the fixed six-job comparison (three without the combiner, then three with
it) with the same 8 map and 4 reduce tasks. Choose a new run ID each time,
because the boss correctly reserves every output directory:

```bash
python -m eval.combiner run --run-id combiner-vm-1 --maps 8 --reduces 4
python -m eval.combiner plot --run-id combiner-vm-1
```

`python -m eval.combiner run` waits for every job, stops on a failed job or incomplete
event log, checks every final output byte against the first no-combiner run,
and appends the six measurements to `eval/results.csv`. The plot command uses
only the six rows for that run ID and writes `eval/plots/combiner.png`. Each
panel shows the median of three runs with minimum/maximum error bars: accepted
mapper intermediate bytes and total job time.
