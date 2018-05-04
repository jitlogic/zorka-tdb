
# Syntax

'NOTE': store search patterns below will be replaces with java based query API that unifies both store search expressions
and regular expressions. Actual query language and its syntax will be implemented in higher layers (Clojure Instaparse). 
Consider this section to be outdated.


Character classes:

* `\d`, `\D` - numeric, non-numeric;

* `\w`, `\W` - word (alphanumeric), non-word;

* `\s`, `\S` - whitespace, non-whitespace;

* `\h`, `\H` - control char (as for FM index) - that is characters less than 21, non-control char;

* `\i`, `\I` - encoded number components (48-112);

* `.` - any character;

* `\c` - quote character `c`, `\Q`, `\E` - beginning and end of quote;

* `\xhh` - quote character by hex code;

* `[]` - defining custom character classes;

 * `a-z` - span;

 * `^` - negation;

 * `&` - conjunction (`and` operator)



Control structures:

* `^` - beginning of regex;

* `$` - end of regex;

* `*` - zero or more;

* `+` - one or more;

* `?` - zero or one;
 
* `{n}`, `{n,m}` - specify number of occurences;



Capture groups:

* `()` - capture groups;

* `|` - alternatives (in capture groups);



# Examples

## Host lookup

Search metadata index for two specific hosts. Fetch trace IDs, host IDs, durations and timestamps.


```
(\i)+\x04(42|8BQw)\x05(\i+)\x06(\i+)\x07
```

Returned groups: `1` traceId, `2` agentId, `3` duration, `4` tstamp. 



# Implementation



