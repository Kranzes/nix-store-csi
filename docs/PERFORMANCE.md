# Performance notes

Measured costs that the plugin does not work around.

## Split L3 caches

The plugin unpacks a NAR on four threads that pass chunks along: one
decompresses, one hashes, one parses and one writes. On CPUs whose L3 cache is
split into slices, like AMD EPYC with one per CCD, a fetch of many NARs puts a
NAR's threads on different slices. Each chunk then crosses between slices or
comes from DRAM. A closure's fetch ends about when its largest NAR's does, so
the whole fetch slows down.

Measured on a 96-core EPYC 9R45, which has 12 slices of 32 MiB, with the plugin
on 88 cores and 16 jobs. The NAR is 3.28 GB, 1.25 GB with zstd, from a local
cache. Counts are per GB of NAR, for the NAR's own threads:

| The NAR                                    | Stream         | CPU            | IPC          | L3 hits    | From another slice | From DRAM    |
| ------------------------------------------ | -------------- | -------------- | ------------ | ---------- | ------------------ | ------------ |
| Alone                                      | 1.88 to 1.91 s | 3.62 to 3.65 s | 2.28 to 2.29 | 29M to 30M | 0.3M to 1.2M       | 4.5M to 7.1M |
| In a fetch of its closure of 291 paths     | 2.22 to 2.37 s | 4.12 to 4.48 s | 1.89 to 2.02 | 6M to 16M  | 10M to 13M         | 13M to 24M   |
| Same, with its threads pinned to one slice | 1.92 to 1.94 s | 3.59 to 3.61 s | 2.29 to 2.31 | 28M to 29M | 2.1M to 2.4M       | 1.7M to 3.7M |

The counts come from a build that gave the NAR threads of its own, to tell
them apart. The plugin itself streams it in 2.40 to 2.54 s in the same fetch.

The slowdown needs several NARs in one process. With two jobs the NAR streams
as fast as alone, and another plugin process fetching the whole closure next
to it does not slow it down. CPUs with one shared L3 do not pay this cost.

On 192-CPU nodes running 0.1.17, fetching a closure of 14.2 GB and 348 NARs
from an in-cluster cache, an AMD EPYC was slowest at 64 jobs in each of three
rounds, with medians of 4.2 s at 16 jobs, 4.1 s at 32 and 4.7 s at 64. On an
Intel Xeon with one shared L3 per socket, the job count made no difference
beyond the spread between rounds.
