---
id: data-structures
sidebar_label: Data Structures
title: Data Structures
---

# Data Structures

## Hash

### HDEL

#### Syntax

```bash
    HDEL key field [field ...]
```


Removes the specified fields from the hash stored at **key**. Specified fields that do not exist within this hash are ignored. If **key** does not exist, it is treated as an empty hash and this command returns 0.

---

### HEXISTS

#### Syntax

```bash
    HEXISTS key field
```

Returns if field is an existing field in the hash stored at **key**.

---

### HGET

#### Syntax

```bash
    HGET key field
```

Returns the value associated with field in the hash stored at **key**.

---

### HGETALL

#### Syntax

```bash
    HGETALL key
```

Returns all fields and values of the hash stored at **key**. In the returned value, every field name is followed by its value, so the length of the reply is twice the size of the hash.

---

### HINCRBY

#### Syntax

```bash
    HINCRBY key field increment
```

Increments the number stored at field in the hash stored at **key** by increment. 
If **key** does not exist, a new **key** holding a hash is created. If field does not exist the value is set to 0 before the operation is performed.
The range of values supported by HINCRBY is limited to 64 bit signed integers.

---

### HINCRBYFLOAT 

#### Syntax

```bash
    HINCRBYFLOAT key field increment
```

Increment the specified field of a hash stored at **key**, and representing a floating point number, by the specified increment. If the increment value is negative, the result is to have the hash field value decremented instead of incremented. If the field does not exist, it is set to 0 before performing the operation. An error is returned if one of the following conditions occur:

* The field contains a value of the wrong type (not a string).
* The current field content or the specified increment are not parsable as a double precision floating point number.

---

### HKEYS

#### Syntax

```bash
    HKEYS key
```

Returns all field names in the hash stored at **key**.

---

### HLEN {#hlen}

#### Syntax

```bash
    HLEN key
```

Returns the number of fields contained in the hash stored at **key**.

---

### HMGET

#### Syntax

```bash 
    HMGET key field [field ...]
```
Ret
urns the values associated with the specified fields in the hash stored at **key**.

For every field that does not exist in the hash, a nil value is returned. Because non-existing keys are treated as empty hashes, running HMGET against a non-existing **key** will return a list of nil values.

### HMSET

#### Syntax

```bash
    HMSET key field value [field value ...]
```

Deprecated in favor of HSET with multiple field-value pairs.

Sets the specified fields to their respective values in the hash stored at **key**. This command overwrites any specified fields already existing in the hash. If **key** does not exist, a new **key** holding a hash is created.

---

### HRANDFIELD

#### Syntax

```bash
    HRANDFIELD key [count [WITHVALUES]]
```

When called with just the **key** argument, return a random field from the hash value stored at **key**.

If the provided count argument is positive, return an array of distinct fields. The array's length is either count or the hash's number of fields ([HLEN](#hlen)), whichever is lower.

If called with a negative count, the behavior changes and the command is allowed to return the same field multiple times. In this case, the number of returned fields is the absolute value of the specified count.

The optional WITHVALUES modifier changes the reply so it includes the respective values of the randomly selected hash fields.

---

### HSCAN {#hscan}

#### Syntax

```bash
    HSCAN key cursor [MATCH pattern] [COUNT count] [NOVALUES]
```

Iterates over the fields and values of a hash stored at a given **key**. Same as [SSCAN](#sscan) and [ZSCAN](#zscan) commands, **HSCAN** is used in order to incrementally iterate over the elements of the hash set*.

The **match** parameter allows to apply a filter to elements after they have been retrieved from the collection. The **count** option sets a limit to the maximum number of items returned from the server to this command. This limit is also set in conjunction with the object-scan-count-limit of the global server settings.

You can use the **NOVALUES** option to make Redis return only the keys in the hash table without their corresponding values

---

### HSET    

#### Syntax

```bash
    HSET key field value
```

Sets the specified fields to their respective values in the hash stored at **key**. This command overwrites the values of specified fields that exist in the hash. If **key** does not exist, a new **key** holding a hash is created. 

---

### HSETNX

#### Syntax

```bash
    HSETNX key field value
```

Sets field in the hash stored at **key** to value, only if field does not yet exist. If **key** does not exist, a new **key** holding a hash is created. If field already exists, this operation has no effect.

---

### HSTRLEN

#### Syntax

```bash
    HSTRLEN key field
```

Returns the string length of the value associated with **field** in the hash stored at **key**. If the **key** or the **field** do not exist, 0 is returned.

---

### HVALS

#### Syntax

```bash 
    HVALS key 
```

Returns all values in the hash stored at **key**.

---

### HEXPIRE

#### Syntax

```bash
    HEXPIRE key seconds [NX | XX | GT | LT] FIELDS numfields field [field ...]
```

Sets a timeout on one or more fields of a hash key. After the timeout has expired, the fields will automatically be deleted. The timeout is specified in seconds.

The command supports several options to control when the expiration should be set:

* **NX:** Only set expiry on fields that have no existing expiry
* **XX:** Only set expiry on fields that already have an expiry set
* **GT:** Only set expiry when it's greater than the current expiry
* **LT:** Only set expiry when it's less than the current expiry

The **NX**, **XX**, **GT**, and **LT** options are mutually exclusive.

#### Resp Reply

Array reply: For each field, returns:

* 1 if the timeout was set
* 0 if the field doesn't exist
* -1 if timeout was not set due to condition not being met

---

### HEXPIREAT

#### Syntax

```bash
    HEXPIREAT key unix-time-seconds [NX | XX | GT | LT] FIELDS numfields field [field ...]
```

Sets an absolute expiration time (Unix timestamp in seconds) for one or more hash fields. After the timestamp has passed, the fields will automatically be deleted.

The command supports several options to control when the expiration should be set:

* **NX:** Only set expiry on fields that have no existing expiry
* **XX:** Only set expiry on fields that already have an expiry set
* **GT:** Only set expiry when it's greater than the current expiry
* **LT:** Only set expiry when it's less than the current expiry

The **NX**, **XX**, **GT**, and **LT** options are mutually exclusive.

#### Resp Reply

Array reply: For each field, returns:

* 1 if the timeout was set
* 0 if the field doesn't exist
* -1 if timeout was not set due to condition not being met

---

### HPEXPIRE

#### Syntax

```bash
    HPEXPIRE key milliseconds [NX | XX | GT | LT] FIELDS numfields field [field ...]
```

Similar to HEXPIRE but the timeout is specified in milliseconds instead of seconds.

The command supports several options to control when the expiration should be set:

* **NX:** Only set expiry on fields that have no existing expiry
* **XX:** Only set expiry on fields that already have an expiry set
* **GT:** Only set expiry when it's greater than the current expiry
* **LT:** Only set expiry when it's less than the current expiry

The **NX**, **XX**, **GT**, and **LT** options are mutually exclusive.

#### Resp Reply

Array reply: For each field, returns:

* 1 if the timeout was set
* 0 if the field doesn't exist
* -1 if timeout was not set due to condition not being met

---

### HPEXPIREAT

#### Syntax

```bash
    HPEXPIREAT key unix-time-milliseconds [NX | XX | GT | LT] FIELDS numfields field [field ...]
```

Similar to HEXPIREAT but uses Unix timestamp in milliseconds instead of seconds.

The command supports several options to control when the expiration should be set:

* **NX:** Only set expiry on fields that have no existing expiry
* **XX:** Only set expiry on fields that already have an expiry set
* **GT:** Only set expiry when it's greater than the current expiry
* **LT:** Only set expiry when it's less than the current expiry

The **NX**, **XX**, **GT**, and **LT** options are mutually exclusive.

#### Resp Reply

Array reply: For each field, returns:

* 1 if the timeout was set
* 0 if the field doesn't exist
* -1 if timeout was not set due to condition not being met

---

### HTTL

#### Syntax

```bash
    HTTL key FIELDS numfields field [field ...]
```

Returns the remaining time to live in seconds for one or more hash fields that have a timeout set.

#### Resp Reply

Array reply: For each field, returns:

* TTL in seconds if the field exists and has an expiry set
* -1 if the field exists but has no expiry set
* -2 if the field does not exist

---

### HPTTL

#### Syntax

```bash
    HPTTL key FIELDS numfields field [field ...]
```

Similar to HTTL but returns the remaining time to live in milliseconds instead of seconds.

#### Resp Reply

Array reply: For each field, returns:

* TTL in milliseconds if the field exists and has an expiry set
* -1 if the field exists but has no expiry set
* -2 if the field does not exist

---

### HEXPIRETIME

#### Syntax

```bash
    HEXPIRETIME key FIELDS numfields field [field ...]
```

Returns the absolute Unix timestamp (in seconds) at which the specified hash fields will expire.

#### Resp Reply

Array reply: For each field, returns:

* Unix timestamp in seconds when the field will expire
* -1 if the field exists but has no expiry set
* -2 if the field does not exist

---

### HPEXPIRETIME

#### Syntax

```bash
    HPEXPIRETIME key FIELDS numfields field [field ...]
```

Similar to HEXPIRETIME but returns the expiry timestamp in milliseconds instead of seconds.

#### Resp Reply

Array reply: For each field, returns:

* Unix timestamp in milliseconds when the field will expire
* -1 if the field exists but has no expiry set
* -2 if the field does not exist

---

### HPERSIST

#### Syntax

```bash
    HPERSIST key FIELDS numfields field [field ...]
```

Removes the expiration from the specified hash fields, making them persistent.

#### Resp Reply

Array reply: For each field, returns:

* 1 if the timeout was removed
* 0 if the field exists but has no timeout
* -1 if the field does not exist

---

### HCOLLECT

#### Syntax

```bash
    HCOLLECT key [key ...]
```

Manualy trigger cleanup of expired field from memory for a given Hash set key.

Use `*` as the key to collect it from all hash keys.

#### Resp Reply

Simple reply: OK response
Error reply: ERR HCOLLECT scan already in progress

---

## List

### BLMOVE

#### Syntax

```bash 
    BLMOVE source destination <LEFT | RIGHT> <LEFT | RIGHT> timeout
```

BLMOVE is the blocking variant of [LMOVE](#lmove-lmove). When source contains elements, this command behaves exactly like LMOVE. When used inside a MULTI/EXEC block, this command behaves exactly like LMOVE. When source is empty, Garnet will block the connection until another client pushes to it or until timeout (a double value specifying the maximum number of seconds to block) is reached. A timeout of zero can be used to block indefinitely.

---

### BRPOPLPUSH

#### Syntax

```bash
BRPOPLPUSH source destination timeout
```

The BRPOPLPUSH command removes the last element from the list stored at source, and pushes the element to the list stored at destination. It then returns the element to the caller.

#### Resp Reply

Bulk string reply: the element being popped and pushed.

---

### BLMPOP

#### Syntax

```bash
    BLMPOP timeout numkeys key [key ...] <LEFT | RIGHT> [COUNT count]
```

BLMPOP is the blocking variant of [LMPOP](#lmpop). When any of the lists contains elements, this command behaves exactly like LMPOP. When used inside a MULTI/EXEC block, this command behaves exactly like LMPOP. When all lists are empty, Garnet will block the connection until another client pushes to it or until timeout (a double value specifying the maximum number of seconds to block) is reached. A timeout of zero can be used to block indefinitely.

---

### BLPOP

#### Syntax

```bash 
    BLPOP key [key ...] timeout
```

BLPOP is a blocking list pop primitive. It is the blocking version of [LPOP](#lpop) because it blocks the connection when there are no elements to pop from any of the given lists. An element is popped from the head of the first list that is non-empty, with the given keys being checked in the order that they are given.

---

### BRPOP

#### Syntax

```bash 
    BRPOP key [key ...] timeout
```

BRPOP is a blocking list pop primitive. It is the blocking version of [RPOP](#rpop) because it blocks the connection when there are no elements to pop from any of the given lists. An element is popped from the tail of the first list that is non-empty, with the given keys being checked in the order that they are given.

---

### LINDEX

#### Syntax

```bash 
    LINDEX key index
```

Removes the element at index index in the list stored at **key**. The index is zero-based. Negative indices can be used to designate elements starting at the tail of the list.


---

### LINSERT

#### Syntax

```bash
    LINSERT key BEFORE|AFTER pivot element
```

Inserts element in the list stored at **key** either before or after the reference value pivot. When key does not exist, it is considered an empty list and no operation is performed.

---

### LLEN

#### Syntax

```bash
    LLEN key
```

Returns the length of the list stored at key. If key does not exist, it is interpreted as an empty list and 0 is returned. 

---

### LMOVE

#### Syntax

```bash
    LMOVE source destination LEFT|RIGHT LEFT|RIGHT
```

Atomically returns and removes the first/last element (head/tail depending on the wherefrom argument) of the list stored at **source**, and pushes the element at the first/last element (head/tail depending on the whereto argument) of the list stored at **destination**.

This command comes in place of the now deprecated RPOPLPUSH. Doing LMOVE RIGHT LEFT is equivalent.

---

### LMPOP

#### Syntax

```bash
    LMPOP numkeys key [key ...] <LEFT | RIGHT> [COUNT count]
```

Pops one or more elements from the first non-empty list key from the list of provided key names.

---

### LPOP

#### Syntax

```bash
    LPOP key [count]
```

Removes and returns the first elements of the list stored at **key**.

By default, the command pops a single element from the beginning of the list. When provided with the optional count argument, the reply will consist of up to count elements, depending on the list's length.

---

### LPOS

#### Syntax

```bash
    LPOS key element [RANK rank] [COUNT num-matches] [MAXLEN len]
```

The command returns the index of matching elements inside a Redis list. By default, when no options are given, it will scan the list from head to tail, looking for the first match of "element". If the element is found, its index (the zero-based position in the list) is returned. Otherwise, if no match is found, nil is returned.

#### Resp Reply

Any of the following:

* Null reply: if there is no matching element.
* Integer reply: an integer representing the matching element.
* Array reply: If the COUNT option is given, an array of integers representing the matching elements (or an empty array if there are no matches).

---

### LPUSH

#### Syntax

```bash 
    LPUSH key [element] [element ...]
```

Insert all the specified values at the head of the list stored at **key**. If **key** does not exist, it is created as empty list before performing the push operations. 

---

### LPUSHX

#### Syntax

```bash
    LPUSHX key [element] [element ...]
```

Inserts specified values at the head of the list stored at **key**, only if **key** already exists and holds a list. In contrary to LPUSH, no operation will be performed when **key** does not yet exist.

---

### LRANGE

#### Syntax

```bash
    LRANGE key start stop
```

Returns the specified elements of the list stored at **key**. The offsets start and stop are zero-based indexes, with 0 being the first element of the list (the head of the list), 1 being the next element and so on.

---

### LREM

#### Syntax

```bash
    LREM key count element
```

Removes the first count occurrences of elements equal to element from the list stored at **key**. The **count** argument influences the operation in the following ways:

* **count > 0:**  Remove elements equal to element moving from head to tail.

* **count < 0:** Remove elements equal to element moving from tail to head.

* **count = 0:** Remove all elements equal to element.

---

### LSET

#### Syntax

```bash
    LSET key index element
```

Sets the list element at **index** to **element**. For more information on the index argument, see [LINDEX](#lindex).

An error is returned for out of range indexes.

---

### LTRIM

#### Syntax

```bash
    LTRIM key start stop
```

Trim an existing list so that it will contain only the specified range of elements specified. Both start and stop are zero-based indexes, where 0 is the first element of the list (the head), 1 the next element and so on.

---

### RPOP

#### Syntax

```bash
    RPOP key [count]
```

Removes and returns the last elements of the list stored at **key**.

By default, the command pops a single element from the end of the list. When provided with the optional count argument, the reply will consist of up to count elements, depending on the list's length.

---

### RPOPLPUSH

#### Syntax

```bash
    RPOPLPUSH source destination
```

Atomically returns and removes the last element (tail) of the list stored at source, and pushes the element at the first element (head) of the list stored at destination.

It can be replaced by [LMOVE](#lmove) with the RIGHT and LEFT arguments when migrating or writing new code.

---

### RPUSH {#rpush}

#### Syntax

```bash
    RPUSH key element [element]
```

Insert all the specified values at the tail of the list stored at **key**. If **key** does not exist, it is created as empty list before performing the push operation. 

---

### RPUSHX

#### Syntax

```bash
    RPUSHX key element [element]
```

Inserts specified values at the tail of the list stored at **key**, only if **key** already exists and holds a list. In contrary to [RPUSH](#rpush), no operation will be performed when **key** does not yet exist. 

---

## Set

### SADD

#### Syntax

```bash
    SADD key member [member]
```

Adds the specified members to the set stored at **key**. Specified members that are already a member of this set are ignored. If **key** does not exist, a new set is created before adding the specified members.

---

### SCARD

#### Syntax

```bash
    SCARD key
```

Returns the set cardinality (number of elements) of the set stored at **key**.

---

### SMEMBERS

#### Syntax

```bash
    SMEMBERS key
```

Returns all the members of the set value stored at **key**.

---

### SMOVE

#### Syntax

```bash
    SMOVE source destination member
```

Move member from the set at source to the set at destination. This operation is atomic. In every given moment the element will appear to be a member of source or destination for other clients.

---

### SPOP

#### Syntax

```bash
    SPOP key [count]
```

Removes and returns one or more random members from the set value stored at **key**.

---

### SISMEMBER

#### Syntax

```bash
    SISMEMBER key member
```

Returns if **member** is a member of the set stored at **key**.

---

### SMISMEMBER

#### Syntax

```bash
    SMISMEMBER key member [member ...]
```

Returns whether each **member** is a member of the set stored at **key**.

#### Resp Reply

Array reply: a list representing the membership of the given elements, in the same order as they are requested.

---

### SRANDMEMBER

#### Syntax

```bash
    SRANDMEMBER key [count]
```

When called with just the **key** argument, return a random element from the set value stored at **key**.

If the provided **count** argument is positive, return an array of **distinct elements**. The array's length is either **count** or the set's cardinality (SCARD), whichever is lower.

If called with a negative **count**, the behavior changes and the command is allowed to return the **same element multiple times**. In this case, the number of returned elements is the absolute value of the specified **count**.

---


### SREM

#### Syntax

```bash
    SREM key member [member]
```

Removes the specified members from the set stored at **key**. Specified members that are not a member of this set are ignored. 

If **key** does not exist, it is treated as an empty set and this command returns 0.

---

### SSCAN

#### Syntax

```bash
    SSCAN key cursor [MATCH pattern] [COUNT count]
```

Iterates elements of Sets types. Same as [HSCAN](#hscan) and [ZSCAN](#zscan) commands, SSCAN is used in order to incrementally iterate over the elements of the set stored at **key**.

The **match** parameter allows to apply a filter to elements after they have been retrieved from the collection. The **count** option sets a limit to the maximum number of items returned from the server to this command. This limit is also set in conjunction with the object-scan-count-limit of the global server settings.

---

### SUNION

#### Syntax

```bash
    SUNION key [key ...]
```

Returns the members of the set resulting from the union of all the given sets.
Keys that do not exist are considered to be empty sets.

---

### SUNIONSTORE

#### Syntax

```bash
    SUNIONSTORE destination key [key ...]
```

This command is equal to [SUNION](#SUNION), but instead of returning the resulting set, it is stored in **destination**.

If **destination** already exists, it is overwritten.

---

### SINTER

#### Syntax

```bash
    SINTER key [key ...]
```

Returns the members of the set resulting from the intersection of all the given sets.
Keys that do not exist are considered to be empty sets.

---

### SINTERSTORE

#### Syntax

```bash
    SINTERSTORE destination key [key ...]
```

This command is equal to [SINTER](#INTER), but instead of returning the resulting set, it is stored in **destination**.

If **destination** already exists, it is overwritten.

---

### SINTERCARD

#### Syntax

```bash
    SINTERCARD numkeys [key ...] [LIMIT limit]
```

Returns the number of members in the resulting set from the intersection of all the given sets.
Keys that do not exist are considered to be empty sets.

The optional `LIMIT` argument specifies an upper bound on the number of intersecting members to count.

---

### SDIFF

#### Syntax

```bash
    SDIFF key [key ...]
```

Returns the members of the set resulting from the difference between the **first** set and all the successive sets. 

**Keys** that do not exist are considered to be empty sets.

---

### SDIFFSTORE

#### Syntax

```bash
    SDIFFSTORE destination key [key ...]
```

This command is equal to [SDIFF](#SDIFF), but instead of returning the resulting set, it is stored in **destination**. 

If **destination** already exists, it is overwritten.

---

## Stream

### XADD

#### Syntax

```bash
    XADD key [NOMKSTREAM] <* | id> field value [field value ...]
```
Appends given stream entry to the stream at specified key. If the key does not exist, it is created when running the command. 
Creation of the stream can be disabled with the `NOMKSTREAM` option. 

Every entry in the stream is accompanied by a stream entry ID and consists of field-value pairs that are stored/read in the same order as provided by the user. 
While the [XADD](#XADD) can auto-generate a unique ID using the `*` character, it is also possible to specify a user-defined ID specified by two 64-bit numbers separated by a `-` character. 
The IDs are guaranteed to be incremental. 

**Capped Streams** are not currently supported. 

---

### XLEN

#### Syntax

```bash
    XLEN key
```
Returns the number of entries inside the stream specified by `key`. If the stream does not exist, returns 0. 

---

### XRANGE

#### Syntax

```bash
    XRANGE key start end [COUNT count]
```
Returns stream entries matching a given range of IDs. 
`start` and `end` can be special IDs (i.e, `-` and `+`) to specify the minimum possible ID and the maximum possible ID inside a stream respectively. 
The IDs provided can also be incomplete (i.e., with only the first part of the ID).
Using the `COUNT` option reduces the number of entries returned. 

### XREVRANGE

#### Syntax

```bash
    XRANGE key end start [COUNT count]
```
Returns stream entries in the order from end to start matching a given range of IDs.
`start` and `end` can be special IDs (i.e, `-` and `+`) to specify the minimum possible ID and the maximum possible ID inside a stream respectively. 
The IDs provided can also be incomplete (i.e., with only the first part of the ID).
Using the `COUNT` option reduces the number of entries returned. 

---

### XDEL

#### Syntax

```bash
    XDEL key id [id ...]
```
Removes the specified entries from a stream given by key, and returns the number of entries deleted. 
If speficied IDs do not exist, the number of entries returned may be less than the number of IDs provided as they are not counted as deleted. 

---

### XTRIM

#### Syntax

```bash
    XTRIM key <MAXLEN | MINID> threshold
```
Trims the stream by evicting older entries using two strategies: 

- MAXLEN: evicts entries as long as stream's length exceeds specified threshold.
- MINID: evicts entries with IDs lower than threshold where `threshold` is an entry ID. 

`LIMIT` clause is not currently supported. 
`MINID` defaults to exact trimming, meaning all entries having IDs lower than threshold will be deleted. 

---

## Sorted Set

### ZADD

#### Syntax

```bash
    ZADD key score member [score member ...]
```

Adds all the specified members with the specified scores to the sorted set stored at **key**. It is possible to specify multiple score / member pairs. If a specified member is already a member of the sorted set, the score is updated and the element reinserted at the right position to ensure the correct ordering.

If key does not exist, a new sorted set with the specified members as sole members is created, like if the sorted set was empty. 

The score values should be the string representation of a double precision floating point number.

---

### ZCARD {#zcard}

#### Syntax

```bash
    ZCARD key
```

Returns the sorted set cardinality (number of elements) of the sorted set stored at **key**.

---

### ZCOUNT

#### Syntax

```bash
    ZCOUNT key min max
```

Returns the number of elements in the sorted set at **key** with a score between min and max.

The min and max arguments have the same semantic as described for [ZRANGEBYSCORE](#zrangebyscore).

---

### ZDIFF

#### Syntax

```bash
    ZDIFF numkeys key [key ...] [WITHSCORES]
```

Returns the difference between the first and all successive input sorted sets keys. The total number of input keys is specified by numkeys.

Keys that do not exist are considered to be empty sets.

---

### ZDIFFSTORE

#### Syntax

```bash
    ZDIFFSTORE destination numkeys key [key ...]
```

Computes the difference between the first and all successive input sorted sets and stores the result in destination. The total number of input keys is specified by numkeys.

Keys that do not exist are considered to be empty sets.

#### Resp Reply

Integer reply: the number of members in the resulting sorted set at destination.

---

### ZINCRBY

#### Syntax

```bash
    ZINCRBY key increment member
```

Increments the score of member in the sorted set stored at **key** by increment. If member does not exist in the sorted set, it is added with increment as its score (as if its previous score was 0.0). If **key** does not exist, a new sorted set with the specified member as its sole member is created.

An error is returned when **key** exists but does not hold a sorted set.

The score value should be the string representation of a numeric value, and accepts double precision floating point numbers. It is possible to provide a negative value to decrement the score.

---

### ZINTER

#### Syntax

```bash
    ZINTER numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE <SUM|MIN|MAX>] [WITHSCORES]
```

Computes the intersection of the sorted sets given by the specified keys and returns the result. It is possible to specify multiple keys.

The result is a new sorted set with the same elements as the input sets, but with scores equal to the sum of the scores of the elements in the input sets.

---

### ZINTERCARD

#### Syntax

```bash
    ZINTERCARD numkeys key [key ...] [LIMIT limit]
```

Returns the number of elements in the intersection of the sorted sets given by the specified keys.

---

### ZINTERSTORE

#### Syntax

```bash
    ZINTERSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE <SUM|MIN|MAX>]
```

Computes the intersection of the sorted sets given by the specified keys and stores the result in the destination key.

---

### ZLEXCOUNT

#### Syntax

```bash
    ZLEXCOUNT key min max
```

When all the elements in a sorted set are inserted with the same score, in order to force lexicographical ordering, this command returns the number of elements in the sorted set at **key** with a value between min and max.

The min and max arguments have the same meaning as described for [ZRANGEBYLEX](#zrangebylex).

---

### ZMSCORE

#### Syntax

```bash
    ZMSCORE key member [member ...]
```

Returns the scores associated with the specified **members** in the sorted set stored at **key**.

For every **member** that does not exist in the sorted set, a nil value is returned.

Returns one of the following:

_Nil reply:_ if the member does not exist in the sorted set.\
_Array reply:_ a list of string **member** scores as double-precision floating point numbers.

---

### BZMPOP

#### Syntax

```bash
    BZMPOP timeout numkeys key [key ...] <MIN | MAX> [COUNT count]
```

BZMPOP is the blocking variant of [ZMPOP](#zmpop). When any of the sorted sets contains elements, this command behaves exactly like ZMPOP. When used inside a MULTI/EXEC block, this command behaves exactly like ZMPOP. When all sorted sets are empty, Garnet will block the connection until another client pushes to it or until timeout (a double value specifying the maximum number of seconds to block) is reached. A timeout of zero can be used to block indefinitely.

- **MIN**: Remove elements starting with the lowest scores
- **MAX**: Remove elements starting with the highest scores
- **COUNT**: Specifies how many elements to pop (default is 1)

#### Resp Reply

One of the following:

* Null reply: when no element could be popped.
* Array reply: a two-element array with the first element being the name of the key from which elements were popped, and the second element is an array of the popped elements. Every entry in the elements array is also an array that contains the member and its score.
---

### BZPOPMAX

#### Syntax

```bash
    BZPOPMAX key [key ...] timeout
```

BZPOPMAX is the blocking variant of [ZPOPMAX](#zpopmax). When any of the sorted sets contains elements, this command behaves exactly like ZPOPMAX. When used inside a MULTI/EXEC block, this command behaves exactly like ZPOPMAX. When all sorted sets are empty, Garnet will block the connection until another client pushes to it or until timeout (a double value specifying the maximum number of seconds to block) is reached. A timeout of zero can be used to block indefinitely.

#### Resp Reply

One of the following:

* Null reply: when no element could be popped and the timeout expired.
* Array reply: the keyname, popped member, and its score.

---

### BZPOPMIN

#### Syntax

```bash
    BZPOPMIN key [key ...] timeout
```

BZPOPMIN is the blocking variant of [ZPOPMIN](#zpopmin). When any of the sorted sets contains elements, this command behaves exactly like ZPOPMIN. When used inside a MULTI/EXEC block, this command behaves exactly like ZPOPMIN. When all sorted sets are empty, Garnet will block the connection until another client pushes to it or until timeout (a double value specifying the maximum number of seconds to block) is reached. A timeout of zero can be used to block indefinitely.

#### Resp Reply

One of the following:

* Null reply: when no element could be popped and the timeout expired.
* Array reply: the keyname, popped member, and its score.

---

### ZMPOP

#### Syntax

```bash
    ZMPOP numkeys key [key ...] <MIN | MAX> [COUNT count]
```

Removes and returns one or more members with the lowest scores (default) or highest scores from the sorted set or sorted sets.

- MIN: Remove elements starting with the lowest scores
- MAX: Remove elements starting with the highest scores
- COUNT: Specifies how many elements to pop (default is 1)

---

### ZPOPMAX

#### Syntax

```bash
    ZPOPMAX key [count]
```

Removes and returns up to count members with the highest scores in the sorted set stored at **key**.

When left unspecified, the default value for count is 1. Specifying a count value that is higher than the sorted set's cardinality will not produce an error. When returning multiple elements, the one with the highest score will be the first, followed by the elements with lower scores.

---

### ZPOPMIN

#### Syntax

```bash
    ZPOPMIN key [count]
```

Removes and returns up to count members with the lowest scores in the sorted set stored at **key**.

When left unspecified, the default value for count is 1. Specifying a count value that is higher than the sorted set's cardinality will not produce an error. When returning multiple elements, the one with the lowest score will be the first, followed by the elements with greater scores.

---

### ZRANDMEMBER

#### Syntax

```bash
    ZRANDMEMBER key [count [WITHSCORES]]
```

When called with just the key argument, return a random element from the sorted set value stored at **key**.

If the provided count argument is positive, return an array of distinct elements. The array's length is either count or the sorted set's cardinality [ZCARD](#zcard), whichever is lower.

If called with a negative count, the behavior changes and the command is allowed to return the same element multiple times. In this case, the number of returned elements is the absolute value of the specified count.

The optional WITHSCORES modifier changes the reply so it includes the respective scores of the randomly selected elements from the sorted set.

---

### ZRANGE {#zrange}

#### Syntax

```bash
    ZRANGE key start stop [BYSCORE|BYLEX] [REV] [LIMIT offset count] [WITHSCORES]
```

Returns the specified range of elements in the sorted set stored at **key**.

**ZRANGE** can perform different types of range queries: by index (rank), by the score, or by lexicographical order.

---

### ZRANGEBYLEX {#zrangebylex}

#### Syntax

```bash
    ZRANGEBYLEX key min max [LIMIT offset count]
```

When all the elements in a sorted set are inserted with the same score, in order to force lexicographical ordering, this command returns all the elements in the sorted set at **key** with a value between min and max.

If the elements in the sorted set have different scores, the returned elements are unspecified.

It can be replaced by [ZRANGE](#zrange) with the BYLEX argument when migrating from older versions.

---

### ZRANGEBYSCORE {#zrangebyscore}

#### Syntax

```bash
    ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
```

Returns all the elements in the sorted set at **key** with a score between min and max (including elements with score equal to min or max). The elements are considered to be ordered from low to high scores.

The elements having the same score are returned in lexicographical order.

It can be replaced by [ZRANGE](#zrange) with the BYSCORE argument when migrating or writing new code.

---

### ZRANK {#zrank}

#### Syntax

```bash
    ZRANK key member [WITHSCORE]
```

Returns the rank of member in the sorted set stored at **key**, with the scores ordered from low to high. The rank (or index) is 0-based, which means that the member with the lowest score has rank 0.

The optional WITHSCORE argument supplements the command's reply with the score of the element returned.

Use [ZREVRANK](#zrevrank) to get the rank of an element with the scores ordered from high to low.

---

### ZREM

#### Syntax

```bash
    ZREM key member [member ...]
```

Removes the specified members from the sorted set stored at **key**. Non existing members are ignored.

---

### ZREMRANGEBYLEX

#### Syntax

```bash
    ZREMRANGEBYLEX key min max
```

When all the elements in a sorted set are inserted with the same score, in order to force lexicographical ordering, this command removes all elements in the sorted set stored at **key** between the lexicographical range specified by min and max.

The meaning of min and max are the same of the [ZRANGEBYLEX](#zrangebylex) command. Similarly, this command actually removes the same elements that [ZRANGEBYLEX](#zrangebylex) would return if called with the same min and max arguments.

---

### ZREVRANGEBYLEX

#### Syntax

```bash
ZREVRANGEBYLEX key max min [LIMIT offset count]
```

The ZREVRANGEBYLEX command returns a range of members in a sorted set, by lexicographical order, ordered from higher to lower strings.

#### Resp Reply

Array reply: list of elements in the specified range.

---

### ZREMRANGEBYSCORE

#### Syntax

```bash
    ZREMRANGEBYSCORE key min max
```

Removes all elements in the sorted set stored at key with a score between min and max (inclusive).

---

### ZREMRANGEBYRANK

#### Syntax

```bash
    ZREMRANGEBYRANK key start stop
```

Removes all elements in the sorted set stored at **key** with rank between start and stop. Both start and stop are 0 -based indexes with 0 being the element with the lowest score. These indexes can be negative numbers, where they indicate offsets starting at the element with the highest score. For example: -1 is the element with the highest score, -2 the element with the second highest score and so forth.

---

### ZREMRANGEBYSCORE

#### Syntax

```bash
    ZREMRANGEBYSCORE key min max
```

Removes all elements in the sorted set stored at **key** with a score between min and max (inclusive).

---

### ZREVRANGE

#### Syntax

```bash
    ZREVRANGE key start stop [WITHSCORES]
```

Returns the specified range of elements in the sorted set stored at **key**. The elements are considered to be ordered from the highest to the lowest score. Descending lexicographical order is used for elements with equal score.

Apart from the reversed ordering, **ZREVRANGE** is similar to [ZRANGE](#zrange).

---

### ZREVRANGEBYSCORE

#### Syntax

```bash
    ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
```

Returns all the elements in the sorted set at key with a score between max and min (including elements with score equal to max or min). 

---

### ZREVRANK {#zrevrank}

#### Syntax

```bash
    ZREVRANK key member [WITHSCORE]
```

Returns the rank of member in the sorted set stored at **key**, with the scores ordered from high to low. The rank (or index) is 0-based, which means that the member with the highest score has rank 0.

The optional WITHSCORE argument supplements the command's reply with the score of the element returned.

Use [ZRANK](#zrank) to get the rank of an element with the scores ordered from low to high.

---

### ZSCAN {#zscan}

#### Syntax

```bash
    ZSCAN key cursor [MATCH pattern] [COUNT count]
```

Iterates over the elements of a Sorted Set. Same as [HSCAN](#hscan) and [SSCAN](#sscan) commands, **ZSCAN** is used in order to incrementally iterate over the elements of the set stored at **key**.

The **match** parameter allows to apply a filter to elements after they have been retrieved from the collection. The **count** option sets a limit to the maximum number of items returned from the server to this command. This limit is also set in conjunction with the object-scan-count-limit of the global server settings.

---

### ZSCORE

#### Syntax

```bash
    ZSCORE key member
```

Returns the score of member in the sorted set at **key**.

If member does not exist in the sorted set, or **key** does not exist, nil is returned.

---

### ZRANGESTORE

#### Syntax

```bash
    ZRANGESTORE dst src min max [BYSCORE|BYLEX] [REV] [LIMIT offset count]
```

Stores the specified range of elements in the sorted set stored at **src** into the sorted set stored at **dst**.

---

### ZUNION

#### Syntax

```bash
    ZUNION numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE <SUM | MIN | MAX>] [WITHSCORES]
```

Returns the union of the input sorted sets specified by the keys. The total number of input keys is specified by numkeys.

Keys that do not exist are considered to be empty sets.

#### Resp Reply

Array reply: the result of the union with, optionally, their scores when WITHSCORES is used.

---

### ZUNIONSTORE

#### Syntax

```bash
    ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE <SUM | MIN | MAX>] 
```

Computes the union of the input sorted sets specified by the keys and stores the result in destination. The total number of input keys is specified by numkeys.

Keys that do not exist are considered to be empty sets.

#### Resp Reply

Integer reply: the number of members in the resulting sorted set at destination.

---

### ZEXPIRE

#### Syntax

```bash
    ZEXPIRE key seconds [NX | XX | GT | LT] MEMBERS nummembers member [member ...]
```

Sets a timeout on one or more members of a sorted set key. After the timeout has expired, the members will automatically be deleted. The timeout is specified in seconds.

The command supports several options to control when the expiration should be set:

* **NX:** Only set expiry on members that have no existing expiry
* **XX:** Only set expiry on members that already have an expiry set
* **GT:** Only set expiry when it's greater than the current expiry
* **LT:** Only set expiry when it's less than the current expiry

The **NX**, **XX**, **GT**, and **LT** options are mutually exclusive.

#### Resp Reply

Array reply: For each member, returns:

* 1 if the timeout was set
* 0 if the member doesn't exist
* -1 if timeout was not set due to condition not being met

---

### ZEXPIREAT

#### Syntax

```bash
    ZEXPIREAT key unix-time-seconds [NX | XX | GT | LT] MEMBERS nummembers member [member ...]
```

Sets an absolute expiration time (Unix timestamp in seconds) for one or more sorted set members. After the timestamp has passed, the members will automatically be deleted.

The command supports several options to control when the expiration should be set:

* **NX:** Only set expiry on members that have no existing expiry
* **XX:** Only set expiry on members that already have an expiry set
* **GT:** Only set expiry when it's greater than the current expiry
* **LT:** Only set expiry when it's less than the current expiry

The **NX**, **XX**, **GT**, and **LT** options are mutually exclusive.

#### Resp Reply

Array reply: For each member, returns:

* 1 if the timeout was set
* 0 if the member doesn't exist
* -1 if timeout was not set due to condition not being met

---

### ZPEXPIRE

#### Syntax

```bash
    ZPEXPIRE key milliseconds [NX | XX | GT | LT] MEMBERS nummembers member [member ...]
```

Similar to HEXPIRE but the timeout is specified in milliseconds instead of seconds.

The command supports several options to control when the expiration should be set:

* **NX:** Only set expiry on members that have no existing expiry
* **XX:** Only set expiry on members that already have an expiry set
* **GT:** Only set expiry when it's greater than the current expiry
* **LT:** Only set expiry when it's less than the current expiry

The **NX**, **XX**, **GT**, and **LT** options are mutually exclusive.

#### Resp Reply

Array reply: For each member, returns:

* 1 if the timeout was set
* 0 if the member doesn't exist
* -1 if timeout was not set due to condition not being met

---

### ZPEXPIREAT

#### Syntax

```bash
    ZPEXPIREAT key unix-time-milliseconds [NX | XX | GT | LT] MEMBERS nummembers member [member ...]
```

Similar to HEXPIREAT but uses Unix timestamp in milliseconds instead of seconds.

The command supports several options to control when the expiration should be set:

* **NX:** Only set expiry on members that have no existing expiry
* **XX:** Only set expiry on members that already have an expiry set
* **GT:** Only set expiry when it's greater than the current expiry
* **LT:** Only set expiry when it's less than the current expiry

The **NX**, **XX**, **GT**, and **LT** options are mutually exclusive.

#### Resp Reply

Array reply: For each member, returns:

* 1 if the timeout was set
* 0 if the member doesn't exist
* -1 if timeout was not set due to condition not being met

---

### ZTTL

#### Syntax

```bash
    ZTTL key MEMBERS nummembers member [member ...]
```

Returns the remaining time to live in seconds for one or more sorted set members that have a timeout set.

#### Resp Reply

Array reply: For each member, returns:

* TTL in seconds if the member exists and has an expiry set
* -1 if the member exists but has no expiry set
* -2 if the member does not exist

---

### ZPTTL

#### Syntax

```bash
    ZPTTL key MEMBERS nummembers member [member ...]
```

Similar to HTTL but returns the remaining time to live in milliseconds instead of seconds.

#### Resp Reply

Array reply: For each member, returns:

* TTL in milliseconds if the member exists and has an expiry set
* -1 if the member exists but has no expiry set
* -2 if the member does not exist

---

### ZEXPIRETIME

#### Syntax

```bash
    ZEXPIRETIME key MEMBERS nummembers member [member ...]
```

Returns the absolute Unix timestamp (in seconds) at which the specified sorted set members will expire.

#### Resp Reply

Array reply: For each member, returns:

* Unix timestamp in seconds when the member will expire
* -1 if the member exists but has no expiry set
* -2 if the member does not exist

---

### ZPEXPIRETIME

#### Syntax

```bash
    ZPEXPIRETIME key MEMBERS nummembers member [member ...]
```

Similar to HEXPIRETIME but returns the expiry timestamp in milliseconds instead of seconds.

#### Resp Reply

Array reply: For each member, returns:

* Unix timestamp in milliseconds when the member will expire
* -1 if the member exists but has no expiry set
* -2 if the member does not exist

---

### ZPERSIST

#### Syntax

```bash
    ZPERSIST key MEMBERS nummembers member [member ...]
```

Removes the expiration from the specified sorted set members, making them persistent.

#### Resp Reply

Array reply: For each member, returns:

* 1 if the timeout was removed
* 0 if the member exists but has no timeout
* -1 if the member does not exist

---

### ZCOLLECT

#### Syntax

```bash
    ZCOLLECT key [key ...]
```

Manualy trigger cleanup of expired member from memory for a given Hash set key.

Use `*` as the key to collect it from all sorted set keys.

#### Resp Reply

Simple reply: OK response
Error reply: ERR ZCOLLECT scan already in progress

---

## Geospatial indices

### GEOADD

#### Syntax

```bash
    GEOADD key [NX | XX] [CH] longitude latitude member [longitude latitude member ... ]
```

Adds the specified geospatial items (longitude, latitude, name) to the specified key. Data is stored into the key as a sorted set, in a way that makes it possible to query the items with the [GEOSEARCH](#geosearch) command.

The command takes arguments in the standard format x,y so the longitude must be specified before the latitude. There are limits to the coordinates that can be indexed: areas very near to the poles are not indexable.

The exact limits, are the following:

* Valid longitudes are from -180 to 180 degrees.
* Valid latitudes are from -85.05112878 to 85.05112878 degrees.

The command will report an error when the user attempts to index coordinates outside the specified ranges.

Note: there is no **GEODEL** command because you can use [ZREM](#zrem) to remove elements. The Geo index structure is a sorted set.

**GEOADD** also provides the following options:

* **XX:** Only update elements that already exist. Never add elements.
* **NX:** Don't update already existing elements. Always add new elements.
* **CH:** Modify the return value from the number of new elements added, to the total number of elements changed (CH is an abbreviation of changed). 

Changed elements are new elements added and elements already existing for which the coordinates was updated. So elements specified in the command line having the same score as they had in the past are not counted. Note: normally, the return value of **GEOADD** only counts the number of new elements added.

Note: The **XX** and **NX** options are mutually exclusive.

---

### GEODIST

#### Syntax

```bash
    GEODIST key member1 member2 [M|KM|FT|MI]
```

Return the distance between two members in the geospatial index represented by the sorted set.

Given a sorted set representing a geospatial index, populated using the GEOADD command, the command returns the distance between the two specified members in the specified unit.

If one or both the members are missing, the command returns NULL.

The unit must be one of the following, and defaults to meters:

* m for meters.
* km for kilometers.
* mi for miles.
* ft for feet.

The distance is computed assuming that the Earth is a perfect sphere, so errors up to 0.5% are possible in edge cases.

---

### GEOHASH

#### Syntax

```bash
    GEOHASH key [member [member ...]]
```

Return valid Geohash strings representing the position of one or more elements in a sorted set value representing a geospatial index (where elements were added using [GEOADD](#geoadd)).

---

### GEOPOS

#### Syntax

```bash
    GEOPOS key [member [member ...]]
```

Return the positions (longitude,latitude) of all the specified members of the geospatial index represented by the sorted set at key.

Given a sorted set representing a geospatial index, populated using the [GEOADD](#geoadd) command, it is often useful to obtain back the coordinates of specified members. When the geospatial index is populated via [GEOADD](#geoadd) the coordinates are converted into a 52 bit geohash, so the coordinates returned may not be exactly the ones used in order to add the elements, but small errors may be introduced.

The command can accept a variable number of arguments so it always returns an array of positions even when a single element is specified.

---

### GEORADIUS

#### Syntax

```bash
GEORADIUS_RO key longitude latitude radius <M | KM | FT | MI>
  [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count [ANY]] [ASC | DESC]
  [STORE key | STOREDIST key]
```

Return the members of a sorted set populated with geospatial information using [GEOADD](#geoadd), which are within the borders of the area specified with the center location and the maximum distance from the center (the radius).

The common use case for this command is to retrieve geospatial items near a specified point not farther than a given amount of meters (or other units). This allows, for example, to suggest mobile users of an application nearby places.

The radius is specified in one of the following units:

* m for meters.
* km for kilometers.
* mi for miles.
* ft for feet.

The command optionally returns additional information using the following options:

* **WITHDIST:** Also return the distance of the returned items from the specified center point. The distance is returned in the same unit specified as the radius argument of the command.
* **WITHCOORD:** Also return the longitude and latitude of the matching items.
* **WITHHASH:** Also return the raw geohash-encoded sorted set score of the item, in the form of a 52 bit unsigned integer. This is only useful for low level hacks or debugging and is otherwise of little interest for the general user.

The command default is to return unsorted items. Two different sorting methods can be invoked using the following two options:

* **ASC:** Sort returned items from the nearest to the farthest, relative to the center point.
* **DESC:** Sort returned items from the farthest to the nearest, relative to the center point.

By default all the matching items are returned. It is possible to limit the results to the first N matching items by using the COUNT option. When ANY is provided the command will return as soon as enough matches are found, so the results may not be the ones closest to the specified point, but on the other hand, the effort invested by the server is significantly lower. When ANY is not provided, the command will perform an effort that is proportional to the number of items matching the specified area and sort them, so to query very large areas with a very small COUNT option may be slow even if just a few results are returned.

By default the command returns the items to the client. It is possible to store the results with one of these options:

* **STORE:** Store the items in a sorted set populated with their geospatial information.
* **STOREDIST:** Store the items in a sorted set populated with their distance from the center as a floating point number, in the same unit specified in the radius.

**Reply**

One of the following:

* If STORE or STOREDIST option is specified, the number of elements in the resulting set (Integer).
* If no WITH* option is specified, an Array reply of matched member names
* If WITHCOORD, WITHDIST, or WITHHASH options are specified, the command returns an Array reply of arrays, where each sub-array represents a single item:
   * The distance from the center as a floating point number, in the same unit specified in the radius.
   * The Geohash integer.
   * The coordinates as a two items x,y array (longitude,latitude).

---

### GEORADIUS_RO

#### Syntax

```bash
GEORADIUS_RO key longitude latitude radius <M | KM | FT | MI>
  [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count [ANY]] [ASC | DESC]
```

Return the members of a sorted set populated with geospatial information using [GEOADD](#geoadd), which are within the borders of the area specified with the center location and the maximum distance from the center (the radius).

The common use case for this command is to retrieve geospatial items near a specified point not farther than a given amount of meters (or other units). This allows, for example, to suggest mobile users of an application nearby places.

The radius is specified in one of the following units:

* m for meters.
* km for kilometers.
* mi for miles.
* ft for feet.

The command optionally returns additional information using the following options:

* **WITHDIST:** Also return the distance of the returned items from the specified center point. The distance is returned in the same unit specified as the radius argument of the command.
* **WITHCOORD:** Also return the longitude and latitude of the matching items.
* **WITHHASH:** Also return the raw geohash-encoded sorted set score of the item, in the form of a 52 bit unsigned integer. This is only useful for low level hacks or debugging and is otherwise of little interest for the general user.

The command default is to return unsorted items. Two different sorting methods can be invoked using the following two options:

* **ASC:** Sort returned items from the nearest to the farthest, relative to the center point.
* **DESC:** Sort returned items from the farthest to the nearest, relative to the center point.

By default all the matching items are returned. It is possible to limit the results to the first N matching items by using the COUNT option. When ANY is provided the command will return as soon as enough matches are found, so the results may not be the ones closest to the specified point, but on the other hand, the effort invested by the server is significantly lower. When ANY is not provided, the command will perform an effort that is proportional to the number of items matching the specified area and sort them, so to query very large areas with a very small COUNT option may be slow even if just a few results are returned.

**Reply**

One of the following:

* If no WITH* option is specified, an Array reply of matched member names
* If WITHCOORD, WITHDIST, or WITHHASH options are specified, the command returns an Array reply of arrays, where each sub-array represents a single item:
   * The distance from the center as a floating point number, in the same unit specified in the radius.
   * The Geohash integer.
   * The coordinates as a two items x,y array (longitude,latitude).
---

### GEORADIUSBYMEMBER

#### Syntax

```bash
GEORADIUSBYMEMBER_RO key member radius <M | KM | FT | MI>
  [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count [ANY]] [ASC | DESC]
  [STORE key | STOREDIST key]
```

This command is exactly like [GEORADIUS](#georadius) with the sole difference that instead of taking, as the center of the area to query, a longitude and latitude value, it takes the name of a member already existing inside the geospatial index represented by the sorted set.

The position of the specified member is used as the center of the query.

---

### GEORADIUSBYMEMBER_RO

#### Syntax

```bash
GEORADIUSBYMEMBER_RO key member radius <M | KM | FT | MI>
  [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count [ANY]] [ASC | DESC]
```

This command is exactly like [GEORADIUS_RO](#georadius_ro) with the sole difference that instead of taking, as the center of the area to query, a longitude and latitude value, it takes the name of a member already existing inside the geospatial index represented by the sorted set.

The position of the specified member is used as the center of the query.

---

### GEOSEARCH

#### Syntax

```bash
    GEOSEARCH key <FROMMEMBER member | FROMLONLAT longitude latitude>
  <BYRADIUS radius <M | KM | FT | MI> | BYBOX width height <M | KM |
  FT | MI>> [ASC | DESC] [COUNT count [ANY]] [WITHCOORD] [WITHDIST]
  [WITHHASH]
```

Return the members of a sorted set populated with geospatial information using [GEOADD](#geoadd), which are within the borders of the area specified by a given shape.

The query's center point is provided by one of these mandatory options:

* **FROMMEMBER:** Use the position of the given existing *member* in the sorted set.
* **FROMLONLAT:** Use the given *longitude* and *latitude* position.

The query's shape is provided by this option:

* **BYRADIUS:** Similar to [GEORADIUS](#georadius), search inside circular area according to given *radius*.
* **BYBOX:** Search inside an axis-aligned rectangle, determined by *height* and *width*.

The command optionally returns additional information using the following options:

* **WITHDIST:** Also return the distance of the returned items from the specified center point. The distance is returned in the same unit as specified for the radius or height and width arguments.
* **WITHCOORD:** Also return the longitude and latitude of the matching items.
* **WITHHASH:** Also return the raw geohash-encoded sorted set score of the item, in the form of a 52 bit unsigned integer. This is only useful for low level hacks or debugging and is otherwise of little interest for the general user.

Matching items are returned unsorted by default. To sort them, use one of the following two options:

* **ASC:** Sort returned items from the nearest to the farthest, relative to the center point.
* **DESC:** Sort returned items from the farthest to the nearest, relative to the center point.

All matching items are returned by default. To limit the results to the first N matching items, use the COUNT *count* option. When the ANY option is used, the command returns as soon as enough matches are found. This means that the results returned may not be the ones closest to the specified point, but the effort invested by the server to generate them is significantly less. When ANY is not provided, the command will perform an effort that is proportional to the number of items matching the specified area and sort them, so to query very large areas with a very small COUNT option may be slow even if just a few results are returned.

**Reply**

One of the following:

* If no WITH* option is specified, an Array reply of matched member names
* If WITHCOORD, WITHDIST, or WITHHASH options are specified, the command returns an Array reply of arrays, where each sub-array represents a single item:
   * The distance from the center as a floating point number, in the same unit specified in the radius.
   * The Geohash integer.
   * The coordinates as a two items x,y array (longitude,latitude).

---

### GEOSEARCHSTORE

#### Syntax

```bash
GEOSEARCHSTORE destination source <FROMMEMBER member |
  FROMLONLAT longitude latitude> <BYRADIUS radius <m | km | ft | mi>
  | BYBOX width height <m | km | ft | mi>> [ASC | DESC] [COUNT count
  [ANY]] [STOREDIST]
```

This command is like [GEOSEARCH](#geosearch), but stores the result in destination key.

When using the STOREDIST option, the command stores the items in a sorted set populated with their distance from the center of the circle or box, as a floating-point number, in the same unit specified for that shape.

**Reply**

Integer reply: the number of elements in the resulting set

---
