

# Store Search API

Store Search API is used by clients (both GUI and REST interface).


## MetadataSearchQuery



## StoreSearchQuery



## Store Search Patterns

'NOTE': store search patterns below will be replaces with java based query API that unifies both store search expressions
and regular expressions. Actual query language and its syntax will be implemented in higher layers (Clojure Instaparse). 
Consider this section to be outdated.

Simplest forms are simple string searches, for example `index.xhtml` - these will search for substring `index.xhtml`
in all searches. Strings can be enclosed using quote character `'` that is useful when string is a part in bigger
expression. When using double quote character, exact matches are enforced instread of substring matches. For example:
`"/index.xhtml"` will match for `/index.xhtml` and not match for `/info/index.xhtml` but either in single quote or
unquoted will match. 

Regular expressions are possible when pattern is prefixed with `~` character, for example: `~index.*ml$` will match for
all strings ending with `index.html`, `index.xml`, `index.xhtml` and similar. Quoting is also possible, with prefix
outside quote, eg. `~'index.*ml'`. When prefix is inside quote (eg. `'~index.html'`), pattern will be interpreted as
simple string starting with tilde character. As with simple substrings, double quote will enforce full match - `~"expr"`
is equivalent for `~'^expr$'`.

### Composite expressions and functions

More sophisticated expressions and functions have lisp-like syntax: `(fn arg1 arg2 ...)`. For example, logical expressions:

* `(& expr1 expr2 ...)` - logical AND;

* `(| expr1 expr2 ...)` - logical OR;

* `(! expr)` - logical inversion;

Searching for key-value pairs is also possible:

* `(= key val)` - fill search for field `key` and value `val`; key must be exact field name or path, value can be any
expression (substring, logical etc.); keys might be dot separated paths when dealing with composite data structures
or `*` for simple mask;

Examples: 

* `(& index. (| action ~.*ml ))`;

* `(= URI ~.*xhtml$)`;

There is shorthand for key-value matches that is useful when searching by hand: `key = val`, 
for example: `SQL = ~'select.*myschema\.orders'`.


