# CachingStore
Basic key-value store with sorted set support (micro-redis).
Can handle multiple simultaneous connections, but the requests are serialized and processed against the datastore by a single worker thread.

## Server JVM variables for configuration:
* initialSize - initial capacity of key-value store (default: 1024)
* addCrLf - set to `true` for server responses to terminate with `\r\n`
* handlerTimeout - seconds until server gives up on processing a command (default: no timeout)
* verbose - set to `true` to enable verbose logging
* debug - set to `true` to enable debug asserts

## Server command arguments
Appliation accepts a single argument, a port number.  Default: 5555

## Supported commands:
* SET key value
* SET key value EX seconds (need not implement other SET options)
* GET key
* DEL key
* DBSIZE
* INCR key
* ZADD key score member
* ZCARD key
* ZRANK key member
* ZRANGE key start stop
