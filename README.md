# One Billion Row Challenge - Go

This is my attempt of the famous [challenge](https://github.com/gunnarmorling/1brc) which was initally announced in January 2024. I went through 8 iterations to get to my current best time.

The code targets Go 1.27. The timings below were measured with Go 1.27.2.

## Timings

Each time is the mean of five benchmark runs.

|Version|Time| Implementation Details                                                            |
|-------|----|-----------------------------------------------------------------------------------|
|v1|1m15s| Initial implementation using idiomatic Go                                         |
|v2|57.3s| Remove `strings.Split`                                                            |
|v3|54.3s| Remove `bufio.Scanner`                                                            |
|v4|38.3s| Use `int64` instead of `float64` and implememt custom `string` to `int64` parser. |
|v5|4.19s| Process file concurrently in chunks                                               |
|v6|1.70s| Reduce `map` access by using pointers.                                            |
|v7|1.55s| Replace `map` with a custom hash bucket using the `fnv` hash                      |
|v8|1.42s| Use PGO to generate optimised binary.                                             |

Detailed benchmarks are included in the `benchmarks` folder.

`v8` is built with its own CPU profile, `profiles/v8.pgo`. `make bench VERSION=v8` passes it to the compiler through `-pgo`. All other versions build with PGO off.

`v5` was noisy, ranging from 2.86s to 4.87s across runs.

## Running the program
This program expects the input file to be present in the `testdata` folder. Please copy-paste the same to the `testdata` folder or create a symlink. You can then use the following command to run the program:

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

`<version>` accepts a valid version number between `v1` and `v8`.

## Performance

Between `v1` and `v8`, I managed to achieve:

* 98.10% drop in execution time from 74.91 seconds to just 1.42 seconds.
* 98.76% drop in memory from ~45GB to just 578MB per operation.
* 99.99% drop in allocations from about 2 billion per operation to about 18 thousand per operation.

It is really pleasing to see how much performance can be gained by careful optimisations which are possible to all the great tools that Go includes as a part of the standard installation.

## NOTE

All the benchmarks were run on battery power on a MacBook Pro with an Apple M5 Max (18 cores: 6 super and 12 performance) and 36GB of RAM, running macOS 27.0.1.
