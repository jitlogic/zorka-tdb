
# FM Index

This is a full text compressed self-index that contains all text and tagging data of collected traces. Data is organized
as integer-string dictionary, in addition dictionary values can have additional additional marking/tagging depending on
what they represent.

FM Index file has 2GB logical (uncompressed) limit. It is enough as there can be many physical files integrated into a
single logical index. 


## Low level tagging 

Raw dictionary data (non-transformed, non-compressed) is encoded in the following way:

```
0x03|id1|0x01|text1|0x02|id1|0x03|id2|0x01|text2|0x02|id2|0x03|.....idN|0x01|textN|0x02|idN|0x03
```

Markers used have their corresponding constants defined in RawDictCodec class:

* `MARK_ID1 = 0x01` - leading ID marker;

* `MARK_TXT = 0x02` - text marker;

* `MARK_ID2 = 0x03` - trailing ID marker;

ID is a variable length integer encoding as described in _Encoding Numbers_ section below.

For each FM file IDs always start from idBase and are incremented by 1. Thus number of dictionary entries can be 
obtained by subtracting last ID from first ID. When looking for ID based on text, we read previous ID and increment it 
(modulo nWords+baseId).

## Index file format (32 bytes)

```
[0]   'ZFM0'  [uint32] - magic & file version
[4]   nblocks [uint32] - number of blocks
[8]   datalen [uint32] - BWT string size (uncompressed);
[12]  nwords  [uint32] - number dictionary records in this file;
[16]  idbase  [uint64] - lowest 32 bits of ID base (will be added to each ID)
[24]  chksum  [uint32] - master checksum (XXH32);
[28]  pidx    [uint32] - index of last character (as returned by BWT transform);

[32]   ranks   [uit32*256] - master rank table;    
    
[32+1024]    bdesc      [nblocks * 14] - block descriptors;    
[32+1024+nblocks*14]    blocks  [nblocks * bytes] - block data
```


### Block descriptor (14 bytes)

```
[0]   poffs [uint32]  - block physical offset (from beginning of file);
[4]   loffs [uint32]  - logical offset (in uncompressed BWT transformed data);
[8]   cksum [uint32]  - checksum (XXH32) for data blocks (both rank and data), character rank for non-data blocks; 
[12]  ranks [uint8]   - number of characters in rank map or character for non-data blocks;
[13]  flags [uint8]   - flags;
```

Block descriptor flags:

* `0x01` - data block if set (type 0 or 1), no-data block if not set (type 2);
 
* `0x02` - data compressed if set, not compressed if not set;


### Data block

Contains compressed data block with variable-size key-value ranking table. 

```
   chars   [nranks * uint8]  - characters (ordered)
   ranks   [nranks * uint32] - character ranks (in the same order as character)
   deltas  [nranks * uint16] - delta ranks (between beginning and end of block);
   data [bytes]              - fragment of BWT transformed raw data compressed;
```


### Non-data block 

Contains special data block consisting of multiple occurences of one and the same character. There is no data 
in this kind of block as all information are encoded in block descriptor.


## Checksums

TODO


# Low level encoding

All multibyte integers are encoded in local byte order. Thus index isn't portable between different architectures.


## Encoding numbers

Numbers are encoded as text in 6-bit encoding starting with '0' (ASCII 48). Encoded numbers start with least significant
parts and end with most significant parts, so that it eases range search.


## Tagging values reference

* `0x00` (0) - index file terminating byte;

* `0x01` (1) - leading ID marker;

* `0x02` (2) - text marker;


## Escaping for illegal characters

* `0x1a` - escaping character, with two subsequent characters  (`0x15` `0x16` `0x17` `0x18` `0x19` `0x1a` - from 0 to 31);

* `0x1b` - represents `\t` (`0x09`)

* `0x1c` - represents `\n` (`0x0a`)

* `0x1d` - represents `\v` (`0x0b`)

* `0x1e` - represents `\f` (`0x0c`) 

* `0x1f` - represents `\r` (`0x0d`)

 
