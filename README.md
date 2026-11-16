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
