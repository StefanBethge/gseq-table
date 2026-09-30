# onebrc_budget

The [One Billion Row Challenge](https://github.com/gunnarmorling/1brc) as a v2 pipeline: read
`station;measurement` lines without a header, cast the measurement, group by station with
min, mean and max, and sort by station, under a memory budget. Sort and group by spill to
disk when the budget is reached.

```sh
go run ./examples/onebrc_budget                  # testdata/measurements.txt
go run ./examples/onebrc_budget -data ~/1brc/golang/measurements.txt
ONEBRC_FILE=~/1brc/golang/measurements.txt go run ./examples/onebrc_budget
```

Run it in `experimental/v2`. Flags:

| Flag | Default | Meaning |
|---|---|---|
| `-data` | `$ONEBRC_FILE`, else `testdata/measurements.txt` | the delivery |
| `-budget` | `0` | memory cap of the run in bytes; `0` uses the budget of the process, a tenth of the memory limit |
| `-block` | `16384` | rows per block |
| `-managed` | `true` | let the engine set `GOMEMLIMIT` to 90 % of the memory limit, unless it is set |
| `-spill` | temporary directory | where the run spills to |

The exit code is the one of the run status: 0 for `ok`, 3 for `delivery_error`, and so on.

## In Docker with memory limits

The budget follows the limit of the container's cgroup. To see it at work, build the example
for Linux and run it with several limits, in `experimental/v2`:

```sh
CGO_ENABLED=0 GOOS=linux go build -o /tmp/onebrc_budget-linux ./examples/onebrc_budget
for mem in 1g 2g 4g; do
  docker run --rm --memory=$mem --memory-swap=$mem \
    -v /tmp:/w -v ~/1brc/golang:/data:ro busybox:1.37.0 \
    /w/onebrc_budget-linux -data /data/measurements.txt -spill /tmp
done
```

`--memory-swap` equal to `--memory` turns swap off, so the limit is the memory of the run. The
file is 13.8 GB and never part of the repository; it is mounted read-only. `bench/docker.sh`
runs the same commands and records runtime and peak memory
(`bench/docker.sh <work dir> onebrc ~/1brc/golang/measurements.txt`).

The test data is a small generated file in the same format (`go run gen.go` in `testdata`), with
placeholders, a decimal comma and a line with a third field, so that the run also rejects rows.

### Results (2026-09-30, after #66)

Apple M4 Pro, Docker Desktop 28.5.1 with 7.7 GiB and 14 CPUs, `GOMEMLIMIT` set by the engine,
budget a tenth of the limit. Details, the measurement before #66 and the other measurements are in
[bench/RESULTS.md](../../bench/RESULTS.md). The runtimes are those of a quiet host; a later run on a
busy host took longer at the same peaks.

| Delivery | Limit | Budget | Result | Runtime | Peak of the process |
|---|---|---:|---|---:|---:|
| full file, 1 billion lines | 1g | 102 MiB | ok | 560 s | 898 MiB |
| full file | 2g | 205 MiB | ok | 363 s | 1300 MiB |
| full file | 4g | 410 MiB | ok | 356 s | 1228 MiB |
| first 20M lines | 1g | 102 MiB | ok | 8 s | 33 MiB |
| first 50M lines | 1g | 102 MiB | ok | 16 s | 70 MiB |
| first 100M lines | 1g | 102 MiB | ok | 35 s | 130 MiB |

Before #66 the full file was killed by the memory limit at every limit, and so were 100M lines, because
the prototype kept data per source row outside the budget (gap G67 of the design set). Now it keeps
bookkeeping only for the rows in the plan, and the group by keeps a running state per station instead
of collecting the rows. What still grows with the file are the rejected rows, which the result holds
without writers in the plan: 710,671 of them in the full file, so the peak at 1g is 89 % of the limit.
For a dirtier delivery, give the pipeline writers for the rejected rows (`RejectsTo`, `OverviewTo`).

The file itself is corrupted: the first 20M lines already hold 9,057 lines (about 0.045 %) that
do not match `name;-?digits.digit`, such as two lines interleaved (`Kankan;40Flores,  Petén;33.9`),
cut lines (`Kuop.0`) and station names made of pieces of others (`Ho Chi Minh CMexicali;28.1`).
Check it with `head -n 20000000 measurements.txt | grep -cvE '^[^;]+;-?[0-9]+\.[0-9]$'`. The
run handles this as designed: it rejects the 7,529 lines it cannot split and the 554 values that
are no number, groups the other lines as they are, and completes with status `ok`.
