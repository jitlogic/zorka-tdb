
# Zorka TracerDB

TracerDB is storage engine for Zorka Internet Collector. TraceDB is implemented as low leve library with minimal 
external dependencies. It implements following features:

* efficient compressed trace data storage - approximately the same compression ratio as in ZICO 1.x achieved by
full normalization of stored data (all strings appear exactly once) and ;

* full-text search across all stored data - enables quick linking of traces from various sources, thus making
distributed systems possible to debug;

* ability to work with partial (chunked) data - opens way to in-flight transaction monitoring (eg. showing locked
transaction and places where they stopped) and new generation, high-performance tracer implementations (by avoiding
server-side data processing);

* low overhead trace data rotation via automatic internal sharding; 


As implementation of (some of) above features is still in progress, this thing is not for general use yet. 

