jc
==

`jc` prints the cardinalities of JSON value paths for a stream of JSON blobs.

Use it like this:

`curl "https://data.nasa.gov/resource/y77d-th95.json" | jc`

Build it like this:

`cargo build`

## HyperLogLog

By default `jc` is a type aware exact count, but can this potentially use up a
lot of memory when the cardinalities are high. Alternatively, you can use the
HyperLogLog algorithm for approximating cardinalities with the `--hll` flag.
However, using HLL means type information is lost, as all values are parsed into
string types.