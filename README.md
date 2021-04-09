# file-feeder
A generally useful adapter for reading from file/s into Diffusion topics.

## Command-line options

### --url <String>
#### Default: ws://localhost:8080
Specify the Diffusion server URL

### --principal <String>
#### Default: control
Username for connecting to Diffusion

### --credentials <String>
#### Default: password
Password for connecting to Diffusion

### --file <String>
#### Default: files
File to be used as the source of data, or directory containing files. Directories are recursively scanned for files, so this option can be used to instantiate a topic tree or branch easily. The file or directory name is used as the topic name.

### --newline
Treat the input files as a source of newline-separated records.

### --delete
Delete files after they have been read. Note that with the --repeat flag, data is still read from an internal cache, so updates still occur.

### --type
#### Default: binary
The type of topic to create, one of:

- string
- JSON
- int64
- double
- binary

### --timeseries
Topics are created as TimeSeries topics. The base type is given by the `--type` flag.

### --publishonly
Set the `PUBLISH_VALUES_ONLY` property on all topics (i.e. disable deltas).

### --dontretain
Set the `DONT_RETAIN_VALUE` property on all topics.

### --sleep <Long>
#### Default: 1000
Specify the time in milliseconds between attempted topic updates. A value of 0 or less means no sleep is performed.

### --stream
Use an `UpdateStream` to update each topic. This will cause the control client to perform delta calculations (unless `--publishonly` is set) rather than the server. Note that only the server currently implements delta calculation backoff under high CPU load; the control client always attempts a full delta calculation.

Without this option, the `TopicUpdate set()` method is used.

Note that this has no effect on TimeSeries topics.

### --batch <Integer>
#### Default: 2147483647
Updates to Diffusion are always acknowledged. At high update rates, the backlog can be significant, leading to client disconnection (queue size exceeded), high memory load and unpredictable performance.

Using this option, we can tune how many outstanding updates we can have before sending further updates; a size of 10-100 seems to give reasonable results.

A value of 1 effectively turns the update into a synchronous operation.

### --repeat
Once all files have been read, continue to update the topics at the rate specified by `--sleep`. If the topic data is cached in memory (--cache), it is not read a second time. This helps with performance.

If `--cache` is specified (see below), all topics in the cache are shuffled into a new random order and sequentially updated. This process is repeated once the last topic has been processed.

If both `--cache` and `--newline` have been specfied, a random record is taken from the same file and used as the new update value.

If `--newline` is not specified then data for each topic is, where possible, chosen from a sibling topic of the one being updated (so topics may still change value even though new data is not read).

Given the topics:

```
A/B/C
A/B/D
A/B/E

```

When updating `A/B/C`, the data may come from `A/B/D` or `A/B/E`, chosen at random.

### --cache
When using --repeat, cache data when it is read from a file. This will prevent reading new files, or changes to files.
Note that reading from the cache is faster and often useful for performance testing.

### --timeseries
Created topics should be time series. The retained range is hardcoded to 1,000.

### --topic <String>
Use a fixed topic name for all updates. This is useful if you want to publish many updates to a single topic when you have a collection of files.

### --prefix <String>
If specified, the given path prefix is prepended to the destination topic name.

### --stripSuffix
Remove the filename suffix (e.g. "json", "txt") from the filename when mapping to a topic name.