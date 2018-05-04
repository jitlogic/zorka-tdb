

# Trace Store Layout

Trace store consists of config file, data file and two indexes (each consisting of potentially many files).
Stores are identified by their numbers, directory containing store files is named as 6-digit hexadecimal number
representing store ID. 


## Configuration file (`config.props`)

Configuration file contains 



# Raw trace store format


Header:

```
[0]  'ZTS0'  [uint32] - magic & file version
[4]  flags   [uint32] - flags (see below);
[8]  cksum   [uint32] - master checksum
[12] ---     [uint32]
```


Flags available:

* `0x01` - LZ4 compressed;

* `0x02` - Zlib compressed;

* `0x10` - CRC32 sums;


Chunk:

```
[0] 'ZCH0' [uint32] - data block magic (for extracting data structure when metadata index gets broken);
[4] csize - compressed chunk size;
[8] lsize - logical (uncompressed) chunk size;;
[12] cksum - data block checksum;
<data>
```


Data is CBOR encoded and compressed and consists of two structures:

* `data` - raw trace (already processed);


# Trace data (TAG=0x08)

Trace data represents method call with optional calls of recursive methods. Method call is represented by sequence.

Sequence can contain method call info maps, attribute maps or recursive method call traces. Note that sequence 
can contain multiple method call info maps or attribute maps. 



## Method call info (mandatory, TAG=0x02)

This is a map that contains numeric keys representing various fields:

* `0x01` - `TRACE_ID` - globally unique ID of a trace;

* `0x02` - `CH_NUM` - chunk number (useful for assembling chunks into full trace);

* `0x03` - `CH_OFFS` - chunk logical offset (offset in logical trace where this chunk begins);

* `0x04` - `CH_LEN` - chunk logical length (uncompressed);

* `0x05` - `TSTAMP` - timestamp (milliseconds since Epoch);

* `0x06` - `DURATION` - trace duration;

* `0x07` - `TYPE` - trace type;

* `0x08` - `RECS` - record count;

* `0x09` - `CALLS` - number of method calls (seen by tracer);

* `0x0a` - `ERRORS` - number of errors (method calls which thrown exceptions);

* `0x0b` - `FLAGS` - flags (error flag etc.);

* `0x0c` - `TSTART` - start timestamp (ticks);

* `0x0d` - `TSTOP` - stop timestamp (ticks);

* `0x0e` - `METHOD` - method description (method ref); 

* `0x0f` - `SKIP` - skip offset (only in postprocessed traces);  



## Custom attributes (optional, TAG=0x03)

Custom attributes is a map tagged with `TAG=0x03`. Keys and values are string refs. Values also can be data
structures but also contain only references to strings. Strings themselves are stored in text index.




