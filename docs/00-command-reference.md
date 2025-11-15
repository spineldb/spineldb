# Command Reference

This document provides a comprehensive list of all commands supported by SpinelDB, categorized by their functionality. SpinelDB aims for Redis compatibility while introducing powerful new commands, especially in the `CACHE` and `JSON` modules.

## Dispatcher Commands

These commands act as dispatchers for various subcommands, often related to specific data types or functionalities.

### `CACHE.*` Commands (Intelligent Caching Engine)

The `CACHE` command provides access to SpinelDB's advanced intelligent caching features.

*   `CACHE.SET key value [TTL seconds] [SWR seconds] [GRACE seconds] [REVALIDATE-URL url] [ETAG etag] [LAST-MODIFIED date] [VARY header-name] [COMPRESSION] [FORCE-DISK] [HEADERS key value ...] [TAGS tag1 tag2 ...]`
*   `CACHE.GET key [REVALIDATE url] [IF-NONE-MATCH etag] [IF-MODIFIED-SINCE date] [FORCE-REVALIDATE] [HEADERS key value ...]`
*   `CACHE.PURGETAG tag1 [tag2 ...]`
*   `CACHE.FETCH key url [TTL seconds] [SWR seconds] [GRACE seconds] [TAGS tag1 tag2 ...] [VARY header-name] [HEADERS key value ...]`
*   `CACHE.STATS`
*   `CACHE.PROXY key [url] [TTL seconds] [SWR seconds] [GRACE seconds] [TAGS tag1 tag2 ...] [VARY header-name] [HEADERS key value ...]`
*   `CACHE.POLICY name [KEY-PATTERN pattern] [URL-TEMPLATE template] [TTL seconds] [SWR seconds] [GRACE seconds] [TAGS tag1 tag2 ...] [PREWARM] [DISALLOW-STATUS-CODES code1 code2 ...] [MAX-SIZE-BYTES size] [VARY-ON header1 header2 ...] [RESPECT-ORIGIN-HEADERS] [NEGATIVE-TTL seconds] [PRIORITY num] [COMPRESSION] [FORCE-DISK]`
*   `CACHE.PURGE pattern1 [pattern2 ...]`
*   `CACHE.LOCK key duration_seconds`
*   `CACHE.UNLOCK key`
*   `CACHE.BYPASS key [duration_seconds]`
*   `CACHE.INFO`
*   `CACHE.SOFTPURGE pattern1 [pattern2 ...]`
*   `CACHE.SOFTPURGETAG tag1 [tag2 ...]`

### `CLUSTER.*` Commands (Clustering)

The `CLUSTER` command provides access to SpinelDB's clustering features.

*   `CLUSTER NODES`
*   `CLUSTER SLOTS`
*   `CLUSTER MYID`
*   `CLUSTER ADDSLOTS slot1 [slot2 ...]`
*   `CLUSTER GETKEYSINSLOT slot count`
*   `CLUSTER MEET ip port`
*   `CLUSTER SETSLOT slot (MIGRATING node_id | IMPORTING node_id | NODE node_id | STABLE)`
*   `CLUSTER REPLICATE master_id`
*   `CLUSTER RESHARD source_node_id destination_node_id slot1 [slot2 ...]`
*   `CLUSTER FORGET node_id`
*   `CLUSTER FIX`

### `JSON.*` Commands (Native JSON Support)

The `JSON` command provides native support for JSON data types.

*   `JSON.ARRAPPEND key path json_value1 [json_value2 ...]`
*   `JSON.ARRINDEX key path json_value [start [end]]`
*   `JSON.ARRINSERT key path index json_value1 [json_value2 ...]`
*   `JSON.ARRLEN key [path]`
*   `JSON.ARRPOP key [path [index]]`
*   `JSON.ARRTRIM key path start stop`
*   `JSON.CLEAR key [path]`
*   `JSON.DEL key [path]`
*   `JSON.GET key [path [path2 ...]]`
*   `JSON.MERGE key path json_value`
*   `JSON.MGET key1 [key2 ...] path`
*   `JSON.NUMINCRBY key path value`
*   `JSON.NUMMULTBY key path value`
*   `JSON.OBJKEYS key [path]`
*   `JSON.OBJLEN key [path]`
*   `JSON.SET key path json_value [NX | XX]`
*   `JSON.STRAPPEND key path json_string`
*   `JSON.STRLEN key [path]`
*   `JSON.TOGGLE key path`
*   `JSON.TYPE key [path]`

### `BF.*` Commands (Bloom Filter)

The `BF` command provides access to SpinelDB's Bloom filter functionality.

*   `BF.RESERVE key error_rate capacity`
*   `BF.ADD key item`
*   `BF.MADD key item [item ...]`
*   `BF.EXISTS key item`
*   `BF.MEXISTS key item [item ...]`
*   `BF.INSERT key [CAPACITY capacity] [ERROR error_rate] ITEMS item [item ...]`
*   `BF.INFO key`
*   `BF.CARD key`

### `PF.*` Commands (HyperLogLog)

The `PF` command provides access to SpinelDB's HyperLogLog functionality.

*   `PFADD key element [element ...]`
*   `PFCOUNT key [key ...]`
*   `PFMERGE destkey sourcekey [sourcekey ...]`

## Standard Commands

These are the top-level commands, often compatible with Redis's standard commands.

### Generic Commands

*   `COMMAND`
*   `CONFIG GET parameter | SET parameter value` (e.g., `CONFIG GET max_clients`)
*   `TYPE key`
*   `PUBSUB subcommand [argument ...]`
*   `PING [message]`
*   `ECHO message`
*   `AUTH password | username password`
*   `SELECT index`
*   `QUIT`
*   `SHUTDOWN [NOSAVE | SAVE]`
*   `DEL key1 [key2 ...]`
*   `UNLINK key1 [key2 ...]`
*   `FLUSHALL [ASYNC]`
*   `FLUSHDB [ASYNC]`
*   `EXPIRE key seconds`
*   `EXPIREAT key timestamp`
*   `PEXPIRE key milliseconds`
*   `PEXPIREAT key milliseconds-timestamp`
*   `TTL key`
*   `PTTL key`
*   `PERSIST key`
*   `KEYS pattern`
*   `DBSIZE`
*   `PUBLISH channel message`
*   `SUBSCRIBE channel1 [channel2 ...]`
*   `PSUBSCRIBE pattern1 [pattern2 ...]`
*   `UNSUBSCRIBE [channel1 ...]`
*   `PUNSUBSCRIBE [pattern1 ...]`
*   `WATCH key1 [key2 ...]`
*   `UNWATCH`
*   `REPLCONF argument [argument ...]`
*   `PSYNC master_replid offset`
*   `INFO [section]`
*   `ASKING`
*   `BGREWRITEAOF`
*   `SORT key [BY pattern] [LIMIT offset count] [GET pattern [GET pattern ...]] [ASC | DESC] [ALPHA] [STORE destination_key]`
*   `EXISTS key1 [key2 ...]`
*   `RENAME key newkey`
*   `RENAMENX key newkey`
*   `SAVE`
*   `BGSAVE`
*   `BACKUP`
*   `CLIENT subcommand [argument ...]`
*   `TIME`
*   `ROLE`
*   `LASTSAVE`
*   `SLOWLOG subcommand [argument ...]`
*   `MEMORY subcommand [argument ...]`
*   `LATENCY subcommand [argument ...]`
*   `MIGRATE host port key | "" destination_db timeout [COPY] [REPLACE] [AUTH password] [AUTH2 username password] [KEYS key1 [key2 ...]]`
*   `RESTORE key ttl serialized-value [REPLACE] [ABSTTL] [IDLETIME seconds] [FREQ seconds]`
*   `SCRIPT subcommand [argument ...]`
*   `EVAL script numkeys key [key ...] arg [arg ...]`
*   `EVALSHA sha1 numkeys key [key ...] arg [arg ...]`
*   `ACL subcommand [argument ...]`
*   `FAILOVER`

### String Commands

*   `GET key`
*   `SET key value [EX seconds] [PX milliseconds] [EXAT timestamp] [PXAT milliseconds-timestamp] [NX | XX] [KEEPTTL] [GET]`
*   `SETEX key seconds value`
*   `PSETEX key milliseconds value`
*   `GETRANGE key start end`
*   `SETRANGE key offset value`
*   `APPEND key value`
*   `INCR key`
*   `DECR key`
*   `INCRBY key increment`
*   `INCRBYFLOAT key increment`
*   `DECRBY key decrement`
*   `MGET key1 [key2 ...]`
*   `MSET key1 value1 [key2 value2 ...]`
*   `MSETNX key1 value1 [key2 value2 ...]`
*   `STRLEN key`
*   `SETBIT key offset value`
*   `GETBIT key offset`
*   `BITCOUNT key [start end]`
*   `BITOP operation destkey key1 [key2 ...]`
*   `BITPOS key bit [start [end]]`
*   `GETDEL key`
*   `GETEX key [EX seconds] [PX milliseconds] [EXAT timestamp] [PXAT milliseconds-timestamp] [PERSIST]`
*   `GETSET key value`
*   `BITFIELD key [GET type offset] [SET type offset value] [INCRBY type offset increment] [OVERFLOW WRAP | SAT | FAIL]`

### List Commands

*   `LPUSH key value1 [value2 ...]`
*   `LPUSHX key value1 [value2 ...]`
*   `RPUSH key value1 [value2 ...]`
*   `RPUSHX key value1 [value2 ...]`
*   `LPOP key [count]`
*   `RPOP key [count]`
*   `LMOVE source destination (LEFT | RIGHT) (LEFT | RIGHT)`
*   `BLMOVE source destination (LEFT | RIGHT) (LEFT | RIGHT) timeout`
*   `LLEN key`
*   `LRANGE key start stop`
*   `LINDEX key index`
*   `LTRIM key start stop`
*   `LINSERT key (BEFORE | AFTER) pivot value`
*   `LSET key index value`
*   `LPOS key element [RANK rank] [COUNT num] [MAXLEN len]`
*   `BLPOP key1 [key2 ...] timeout`
*   `BRPOP key1 [key2 ...] timeout`
*   `LREM key count value`

### Hash Commands

*   `HSET key field value [field value ...]`
*   `HGET key field`
*   `HGETALL key`
*   `HDEL key field1 [field2 ...]`
*   `HEXISTS key field`
*   `HLEN key`
*   `HKEYS key`
*   `HVALS key`
*   `HINCRBY key field increment`
*   `HINCRBYFLOAT key field increment`
*   `HMGET key field1 [field2 ...]`
*   `HRANDFIELD key [count [WITHVALUES]]`
*   `HSETNX key field value`
*   `HSTRLEN key field`

### Set Commands

*   `SADD key member1 [member2 ...]`
*   `SREM key member1 [member2 ...]`
*   `SMEMBERS key`
*   `SISMEMBER key member`
*   `SCARD key`
*   `SPOP key [count]`
*   `SUNION key1 [key2 ...]`
*   `SINTER key1 [key2 ...]`
*   `SDIFF key1 [key2 ...]`
*   `SRANDMEMBER key [count]`
*   `SMOVE source destination member`
*   `SMISMEMBER key member1 [member2 ...]`
*   `SUNIONSTORE destination key1 [key2 ...]`
*   `SINTERSTORE destination key1 [key2 ...]`
*   `SDIFFSTORE destination key1 [key2 ...]`

### Sorted Set Commands

*   `ZADD key [NX | XX | CH | INCR] [score member [score member ...]]`
*   `ZRANGE key start stop [BYSCORE | BYLEX] [REV] [LIMIT offset count] [WITHSCORES]`
*   `ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]`
*   `ZRANGEBYLEX key min max [LIMIT offset count]`
*   `ZREVRANGE key start stop [WITHSCORES]`
*   `ZCARD key`
*   `ZSCORE key member`
*   `ZCOUNT key min max`
*   `ZLEXCOUNT key min max`
   `ZREM key member1 [member2 ...]`
*   `ZREMRANGEBYSCORE key min max`
*   `ZRANK key member`
*   `ZREVRANK key member`
*   `ZINCRBY key increment member`
*   `ZPOPMIN key [count]`
*   `ZPOPMAX key [count]`
*   `BZPOPMIN key1 [key2 ...] timeout`
*   `BZPOPMAX key1 [key2 ...] timeout`
*   `ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM | MIN | MAX]`
*   `ZINTERSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM | MIN | MAX]`
*   `ZREMRANGEBYLEX key min max`
*   `ZREMRANGEBYRANK key start stop`
*   `ZRANGESTORE dst src min max [BYSCORE | BYLEX] [REV] [LIMIT offset count]`
*   `ZMSCORE key member1 [member2 ...]`

### Geospatial Commands

*   `GEOADD key longitude latitude member [longitude latitude member ...]`
*   `GEOPOS key member1 [member2 ...]`
*   `GEODIST key member1 member2 [unit]`
*   `GEORADIUS key longitude latitude radius M|KM|FT|MI [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count] [ASC|DESC] [STORE key] [STOREDIST key]`
*   `GEORADIUSBYMEMBER key member radius M|KM|FT|MI [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count] [ASC|DESC] [STORE key] [STOREDIST key]`

### Stream Commands

*   `XADD key ID field value [field value ...]`
*   `XRANGE key start end [COUNT count]`
*   `XREVRANGE key end start [COUNT count]`
*   `XTRIM key MAXLEN | MINID [~ | =] threshold`
*   `XDEL key ID1 [ID2 ...]`
*   `XLEN key`
*   `XINFO subcommand [argument ...]`
*   `XGROUP subcommand [argument ...]`
*   `XACK key group ID1 [ID2 ...]`
*   `XPENDING key group [start end count [consumer]]`
*   `XCLAIM key group consumer min-idle-time ID1 [ID2 ...] [JUSTID] [FORCE] [LASTID ID] [RETRYCOUNT count]`
*   `XREAD [COUNT count] [BLOCK milliseconds] STREAMS key1 [key2 ...] ID1 [ID2 ...]`
*   `XREADGROUP GROUP group consumer [COUNT count] [BLOCK milliseconds] [NOACK] STREAMS key1 [key2 ...] ID1 [ID2 ...]`
*   `XAUTOCLAIM key group consumer min-idle-time start-id [COUNT count] [JUSTID]`

### Scan Commands

*   `SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]`
*   `HSCAN key cursor [MATCH pattern] [COUNT count]`
*   `SSCAN key cursor [MATCH pattern] [COUNT count]`
*   `ZSCAN key cursor [MATCH pattern] [COUNT count]`

---

<div align="right">
  ➡️ <strong>Next Chapter: <a href="./01-installation-and-setup.md">1. Installation & Setup</a></strong>
</div>