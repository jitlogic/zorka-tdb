
# Text Index high level encoding

Text Index contains potentially reusable data.  


## Typed objects

Special prefix for special objects: 

* `0x04|ns/keyword|0x04` - LISP keyword (ns part is optional); 

* `0x05|classname|0x05` - class name;

* `0x06|methodname|0x06` - method name;

* `0x07|uuid|0x07` - UUID;




## Key-Value pairs

Key-Value pairs are encoded as tuples consisting of key and value:

```
0x08|idk|0x08|value|0x08
0x09|idk|0x09|idv|0x09
```

Where:

* `idk` - key ID, always refers to a keyword or tuple of keywords;

* `value` - value, refers to any data type;

* `idv` - value ID;

Both key and value are encoded as separate id-text pairs in order to facilitate full text search.

For each key-value pair both pair ID and raw value ID are associated with individual traces.

For path-value pairs both path-value pair ID and key-value pair (where key is last element of a path) are associated with 
individual traces.

Trace type is represented as a standard key-value pair. 


## Maps

Maps are encoded as tuples of pair IDs. IDs of such tuple are sorted in ascending order. Maps can be untyped or typed.
Typed maps are prefixed with `0x06|t` pair. 
 

## Methods and Classes

Class names, method names are encoded as symbols. Method signatures are encoded as strings. Full method definition 
consists of class ID, method ID and signature ID and is encoded as a tuple of `class_id`, `method_id`, `signature_id`.

```
0x0A|cid|0x0A|mid|0x0A|sid|0x0A
```

Only full method definitions IDs are associated with individual traces. 

Possible optimization: associate only those definions that represents calls that are long enough or errors. 


## Encoding exceptions

Exceptions are normalized into 3-level structures. This greatly improves search capabilities and compression ratio. 
On top level exceptions are represented as a tuple:

```
0x0b|class_id|0x0b|msg_id|0x0b|stack_id|0x0b|cause_id|0x0b
```

Where `class_id` refers to class name symbol, `msg_id` refers to message string, `stack_id` to stack trace (see below), 
`cause_id` refers to wrapped exception (if any). If exception has no cause, then `cause_id` empty.


Stack trace is encoded as a tuple:

```
0x0c|stid1|0x0c|stid2|0x0c|...|stidN|00c
```

Each stack trace element is also encoded as a tuple:

```
0x0d|class_id|0x0d|method_id|0x0d|file_id|0x0d|line_num|0x0d
```

In trace data exceptions are thus represented by single IDs. Exception IDs are associated with trace metadata. 


## Encoding host and agent identifications

Agents are identified by UUIDs and described as maps of attributes (UUID being one of them).

Agent UUID ID is associated with individual traces.  

```
0x0e|idh|0x0e|idk|0x0e|value
```

Agent attribute IDs are associted with individual traces.

