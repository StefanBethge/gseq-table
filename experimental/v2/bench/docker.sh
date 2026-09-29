#!/bin/sh
# Runs the measurements in Docker with memory limits (T23, G13, D60).
#
#   bench/docker.sh <work dir> <plan> [1BRC file]
#
# Run it in experimental/v2. The work dir holds the Linux binaries, the
# generated deliveries (data/, from `bench suite` or `bench gen`) and the
# results (docker.jsonl). Plans:
#
#   sort     sort of 10M numeric-heavy rows to a sink, with and without raw state and
#            GOMEMLIMIT set by the engine, under 1g, 2g and 4g
#   onebrc   examples/onebrc_budget over the 1BRC file under 1g, 2g and 4g
#
# The 1BRC file is mounted read-only and never copied. Swap is off
# (--memory-swap equals --memory), so the limit is the memory of the run.
set -eu
work=$1
plan=$2
limits=${LIMITS:-"1g 2g 4g"}
CGO_ENABLED=0 GOOS=linux go build -o "$work/bench-linux" ./bench
CGO_ENABLED=0 GOOS=linux go build -o "$work/onebrc_budget-linux" ./examples/onebrc_budget

in_docker() {
	mem=$1
	shift
	docker run --rm --memory="$mem" --memory-swap="$mem" -v "$work":/w "$@"
}

case $plan in
sort)
	for mem in $limits; do
		for impl in v2 v2noraw; do
			for managed in "" -managed; do
				in_docker "$mem" busybox:1.37.0 /w/bench-linux exec -label "docker-sort-$mem-$impl" $managed -out /w/docker.jsonl -- \
					/w/bench-linux run -impl $impl -case sort -kind num -rows 10000000 -dir /w/data -spill /tmp -sink $managed || true
			done
		done
	done
	;;
onebrc)
	file=$3
	for mem in $limits; do
		in_docker "$mem" -v "$(dirname "$file")":/data:ro busybox:1.37.0 /w/bench-linux exec -label "docker-onebrc-$mem" -managed -out /w/docker.jsonl -- \
			/w/onebrc_budget-linux -data "/data/$(basename "$file")" -spill /tmp || true
	done
	;;
*)
	echo "unknown plan $plan" >&2
	exit 2
	;;
esac
