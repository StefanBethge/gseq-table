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
| `-budget` | `0` | memory cap of the run in bytes; `0` uses the budget of the process, a quarter of the memory limit |
| `-block` | `16384` | rows per block |
| `-managed` | `true` | let the engine set `GOMEMLIMIT` to 90 % of the memory limit, unless it is set |
| `-spill` | temporary directory | where the run spills to |

The exit code is the one of the run status: 0 for `ok`, 3 for `delivery_error`, and so on.

## In Docker with memory limits

The budget follows the limit of the container's cgroup. To see it at work, run the real
file with several limits, from the root of the repository:

```sh
for mem in 1g 2g 4g; do
  docker run --rm --memory=$mem \
    -v "$PWD":/src -v ~/1brc/golang:/data:ro -w /src/experimental/v2 \
    golang:1.27 go run ./examples/onebrc_budget -data /data/measurements.txt
done
```

The file is 13.8 GB and never part of the repository. The test data is a small generated
file in the same format (`go run gen.go` in `testdata`), with placeholders, a decimal comma
and a line with a third field, so that the run also rejects rows.

The prototype keeps some data per source row outside the budget, such as the counts and the
locations of the rows. At the size of the real file this can be more than the smaller limits
allow; the measurements in Docker are part of slice 9 (#53).
