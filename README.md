# One Billion Row Challenge - Go

This is my attempt of the famous [challenge](https://github.com/gunnarmorling/1brc) which was initally announced in January 2024. I went through 7 iterations to get to my current best time.

## Timings

|Version|Time| Implementation Details                                                            |
|-------|----|-----------------------------------------------------------------------------------|
|v1|2m26s| Initial implementation using idiomatic Go                                         |
|v2|1m42s| Remove `strings.Split`                                                            |
|v3|1m39s| Remove `bufio.Scanner`                                                            |
|v4|1m17s| Use `int64` instead of `float64` and implememt custom `string` to `int64` parser. |
|v5|11s| Process file concurrently in chunks                                               |
|v6|5.7s| Reduce `map` access by using pointers.                                            |
|v7|3.2s| Replace `map` with a custom hash bucket using the `fnv` hash                      |

Detailed benchmarks are included in the `benchmarks` folder.

## Running the program

```bash
make run VERSION=<version>
```

## Tests and Benchmarks

```bash
make test
```

```bash
make bench VERSION=<version>
```

`<version>` accepts a valid version number between `v1` and `v7`.

## Performance

Between `v1` and `v7`, I managed to achieve:

* 97.69% drop in execution time from 148.46 seconds to just 3.4 seconds!
* 99.31% drop in the memory from ~45GB to just 321MB.
* More than 100% drop in allocations from 2GB per operation to just 11kb per operation.

It is really pleasing to see how much performance can be gained by careful optimisations which are possible to all the great tools that Go includes as a part of the standard installation.