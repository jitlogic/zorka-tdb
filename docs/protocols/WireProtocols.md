
# Wire trace data

Trace data is submitted via collector HTTP API described in last section. 

## Trace Envelope

Envelope is a top level structure transmitted from agent to collector. 

## Trace Records

Raw trace is a data stream produced directly by tracer. This format is optimized for agents in order to minimize 
agent overhead but in doing so it requires some post processing in collector.  

Each trace is encoded as variable-length array containing information on current method call:

```
(TAG=0x0a/0x0b)[prolog,begin?,attr1?,attr2?,...,sub1,sub2...,attrn?,attrn+1?,...,exception?,epilog]
```

Both prolog and epilog contain data in architecture dependent byte order. If `TAG=6` this is big endian, for `TAG=7`
it's little endian (most popular architectures, notably x86).

Prolog is a CBOR byte array that contains one 64-bit word containing following data:

    0                  40          64
    +-------------------+-----------+
    |tstamp (call start)|method_id  |
    +-------------------+-----------+

* `tstamp` - is number of ticks since JVM start; a tick is calculated as `System.nanoTime() >> 16` (2^16 ns);

* `method_id` - method ID as assigned when instrumenting class;


Epilog is tagged CBOR byte array that contains one 64-bit word (with optional second word) containing following data:

             0                  40          64                              128
             +-------------------+-----------+-------------------------------+
    (TAG=0d) |tstamp (call end)  |calls      | long_calls (optional)         |
             +-------------------+-----------+-------------------------------+

* `tstamp` - is number of ticks since JVM start; a tick is calculated as `System.nanoTime() >> 16` (2^16 ns);

* `calls` - number of (instrumented) method calls (1 if no methods were called from current method); 
if this number exceeds `2^24-1`, it is set to 0 and 64-bit variant is appended (`long_calls`);


After prolog there can be arbitrary number of attribute maps or subordinate trace records. Also, exception can be 
added if thrown by method. Trace can mix arbitrary 

## TraceBegin

TraceBegin structure marks beginning of a trace. 

```
(TAG=33)[clock,traceId]
```

* `clock` - current time (as from `System.currentTimeMillis()`);

* `traceId` - trace ID (as an integer) - string ref;


## Attributes

Normal attributes are attached to current method:

```
(TAG=0x09){key1,val1,key2,val2,...keyN,valN}
```

Where both keys and values can be of arbitrary (possibly recursive) type. Note that both keys or value can be or 
contain string-refs and similar types.

Upward attributes can be attached to upward method that marks start of a trace:

```
(TAG=0x26)[traceId,{key1,val1,key2,val2,...keyN,valN}]
```

If `traceId` = 0, attributes will be attached to any upwards record in the stack. If traceID != 0, appropriate trace
top will be selected. 

Note that upward attributes 


## Exceptions

Full exception info is encoded as tagged 5-element array:

```
(TAG=34)[id,class,message,cause,stack]
```

* `id` - (not certainly unique ID) result of `System.identityHashCode(e)`; 

* `class` - exception class name (string or string-ref);

* `message` - exception message (string);

* `cause` - cause ID;

* `stack` - array of stack trace elements;

Each stack element is encoded as 4-element array:

```
[class,method,file,line]
```
 
* `class` - class name (string ref);

* `method` - method name (string ref);

* `file` - file name (string ref);

* `line` - line number (integer);


## String refs, typed data

TBD



# Wire Agent Data

Data block sent from agent to server is a sequence of tuples of two types:

* string ref definition (`TAG=0x0d`) - defines (typed) string reference;

* method ref definition (`TAG=0x0e`) - defines method triple (consisting of class, method and signature refs); 


Data is submitted via HTTP `/submit/agent` URI with the following parameters:

* `host` - host UUID;

* `data` - CBOR data encoded in base64 (use either this parameter or one of following);

* `zdata` - zlib compressed CBOR data encoded in base64;

* `ldata` - LZ4 compressed CBOR data encoded in base64;

## String ref (TAG=0x0d)

```
(TAG=0x0d)[id,s,type]
```

Where:

* `id` - local ID (assigned by agent)

* `s` - string;

* `type` - int value determining string type;


Available types:

* `0` - untyped string;

* `4` - LISP keyword;

* `5` - class name;

* `6` - method name;

* `7` - UUID;

* `8` - method signature;


## Method ref (TAG=0x0e)

```
(TAG=0x0e)[id,class,method,signature]
```

Where:

* `id` - local ID (assigned by agent);

* `class` - class name ref (must be of type `2`);

* `method` - method name ref (must be of type `3`);

* `signature` - method signature ref (must be of type `4`);


## Agent attributes (TAG=0x0f)

```
(TAG=0x0f)[key,val]
```

Where:

* `key` - attribute key (string);

* `val` - attribute value (string);



# ZICO Collector HTTP API

## Agent registration: `/agent/register`


POST request with `application/json` or `application/edn` map with following keys:

* `rkey` - registration key (can be distributed)

* `name` - agent name (eg. `zorka.hostname`);

* `uuid` - agent UUID (optional: pass if previously registered); 

* `akey` - authentication key (for previously registered agents)

* `app` - application (name, not UUID);

* `env` - environment (name, not UUID);

* `attrs` - custom agent attribute map (optional);


Returns map containing following fields:

* `uuid` - host uuid;

* `authkey` - authentication key (to be used later )


Status codes:

* `200` - success (host already registered);

* `201` - success (new host registered);

* `400` - arguments missing or invalid;

* `401` - access denied;

* `500` - internal error (in collector);


## Authenticating and creating session `/agent/session`

POST request `application/json` or `application/edn` map with following keys:

* `uuid` - agent UUID (obtained from `/agent/register`);

* `authkey` - authentication key (obtained from `/agent/register`);


Returns map containing following fields:

* `session` - session UUID;


Status codes:

* `200` - success;

* `400` - missing arguments;

* `401` - access denied;


## Submitting agent metadata

TBD


## Submitting traces

TBD

