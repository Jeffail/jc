jc
==

`jc` prints the cardinalities of JSON value paths for a stream of JSON blobs.

Use it like this:

`cat ./a_bunch_of_line_delim.json | jc`

Or, like this:

``` sh
$ jc
{"foo":{"bar":1}}
{"foo":{"bar":2}}
{"foo":{"bar":[3,4,5]}}
<CTRL+D>
{"foo.bar":5}
```

Build it like this:

`cargo build`

## HyperLogLog

By default `jc` is a type aware exact count, but this can potentially use up a
lot of memory when the cardinalities are high. Alternatively, you can use the
HyperLogLog algorithm for approximating cardinalities with the `--hll` flag.
However, using HLL means type information is lost, as all values are parsed into
string types.
