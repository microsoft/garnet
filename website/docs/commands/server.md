---
id: server-commands
sidebar_label: Server Management
title: Server Management
slug: server
---


### COMMAND
#### Syntax

```bash
COMMAND
```

Return an array with details about every Garnet command.

#### Resp Reply

Array reply: a nested list of command details.

---
### COMMAND COUNT
#### Syntax

```bash
COMMAND COUNT
```

Returns Integer reply of number of total commands in this Garnet server.

#### Resp Reply

Integer reply: the number of commands returned by COMMAND.

---
### COMMAND DOCS
#### Syntax

```bash
COMMAND DOCS [command-name [command-name ...]]
```

Return documentary information about commands.

By default, the reply includes all of the server's commands. You can use the optional command-name argument to specify the names of one or more commands.

#### Resp Reply

Array reply: a map, as a flattened array, where each key is a command name, and each value is the documentary information.

---
### COMMAND GETKEYS
#### Syntax

```bash
COMMAND GETKEYS command-name [arg [arg ...]]
```

Returns an array of keys that would be accessed by the given command.

* `command-name`: The name of the command to analyze
* `arg`: The arguments that would be passed to the command

#### Resp Reply

Array reply: a list of keys that the command would access.

---
### COMMAND GETKEYSANDFLAGS
#### Syntax

```bash
COMMAND GETKEYSANDFLAGS command-name [arg [arg ...]]
```

Returns an array of key names and access flags for keys that would be accessed by the given command.

* `command-name`: The name of the command to analyze
* `arg`: The arguments that would be passed to the command

#### Resp Reply

Array reply: a nested array where each item contains:
1. The key name
2. An array of access flag strings that apply to that key

---
### COMMAND INFO
#### Syntax

```bash
COMMAND INFO [command-name [command-name ...]]
```

Returns Array reply of details about multiple Garnet commands.

Same result format as COMMAND except you can specify which commands get returned.

If you request details about non-existing commands, their return position will be nil.

#### Resp Reply

Array reply: a nested list of command details.

---
### COMMITAOF
#### Syntax

```bash
COMMITAOF
```

This command manually issues a commit to write ahead logging (append-only file)

#### Resp Reply

Simple string reply: AOF file committed

---
### CONFIG GET
#### Syntax

```bash
CONFIG GET parameter [parameter ...]
```

The CONFIG GET command is used to read the configuration parameters of a running Garnet server.

#### Resp Reply

Array reply: a list of configuration parameters matching the provided arguments.

---
### CONFIG SET
#### Syntax

```bash
CONFIG SET parameter value [parameter value ...]
```

The CONFIG SET command is used in order to reconfigure the server at run time without the need to restart Garnet.

#### Resp Reply

Simple string reply: OK when the configuration was set properly. Otherwise an error is returned.

---
### DBSIZE
#### Syntax

```bash
DBSIZE
```

Return the number of keys in the currently-selected database.

#### Resp Reply

Integer reply: the number of keys in the currently-selected database.

---
### DEBUG
#### Syntax

```bash
DEBUG [subcommand [...]]
```

The DEBUG command is an internal command. It is meant to be used for developing and testing the server and its clients. See DEBUG HELP for subcommand list. It's disabled by default unless EnableDebugCommand option is set or --enable-debug-command command line option is used.

---
### FLUSHALL
#### Syntax

```bash
FLUSHALL [ASYNC | SYNC]
```

Delete all the keys of all the existing databases, not just the currently selected one. This command never fails.

#### Resp Reply

Simple string reply: OK.

---
### FLUSHDB
#### Syntax

```bash
FLUSHDB [ASYNC | SYNC]
```

Delete all the keys of the currently selected DB. This command never fails.

#### Resp Reply

Simple string reply: OK.

---
### LATENCY HELP
#### Syntax

```bash
LATENCY HELP
```

Returns all the supported LATENCY sub-commands

#### Resp Reply

Array reply: a list of LATENCY supported sub-command details.

---
### LATENCY HISTOGRAM
#### Syntax

```bash
LATENCY HISTOGRAM [event [event ...]]
```

Return latency histogram of or more ```<event>``` classes. \
If no commands are specified then all histograms are replied

#### Resp Reply

Array reply

---
### LATENCY RESET
#### Syntax

```bash
LATENCY RESET [event [event ...]]
```

Reset latency data of one or more ```<event>``` (default: reset all data for all event classes).

#### Resp Reply

Simple string reply: OK.

---
### SLOWLOG GET
#### Syntax

```bash
SLOWLOG GET [count]
```

Returns entries in the slow log. The default is to return the latest 10 entries. Use a negative count to return all entries.

---
### SLOWLOG LEN
#### Syntax
```bash
SLOWLOG LEN
```

Returns the length of the slow queries log.

---
### SLOWLOG RESET
#### Syntax
```bash
SLOWLOG RESET
```

Reset the slow log (discard all existing entries).

---
### SLOWLOG HELP
#### Syntax
```bash
SLOWLOG HELP
```

Returns a list of supported SLOWLOG sub-commands.

---
### MEMORY USAGE
#### Syntax

```bash
MEMORY USAGE key [SAMPLES count]
```

The MEMORY USAGE command reports the number of bytes that a key and its value require to be stored in RAM.

#### Resp Reply

One of the following:
* Integer reply: the memory usage in bytes.
* Null reply: if the key does not exist.

---

### REPLICAOF

#### Syntax

```bash
REPLICAOF <host port | NO ONE>
```

The REPLICAOF command can change the replication settings of a replica on the fly.

#### Resp Reply

Simple string reply: OK.

---

### ROLE

#### Syntax

```bash
ROLE
```

Provide information on the role of a Redis instance in the context of replication, by returning if the instance is currently a master, slave, or sentinel. The command also returns additional information about the state of the replication (if the role is master or slave) or the list of monitored master names (if the role is sentinel).

#### Resp Reply

The command returns an array of elements. The elements of the array depends on the role.

## Master output

The master output is composed of the following parts:

1. The string `master`.
2. The current master replication offset, which is an offset that masters and replicas share to understand, in partial resynchronizations, the part of the replication stream the replicas needs to fetch to continue.
3. An array composed of three elements array representing the connected replicas. Every sub-array contains the replica IP, port, and the last acknowledged replication offset.

## Output of the command on replicas

The replica output is composed of the following parts:

1. The string `slave`, because of backward compatibility (see note at the end of this page).
2. The IP of the master.
3. The port number of the master.
4. The state of the replication from the point of view of the master, that can be `connect` (the instance needs to connect to its master), `connecting` (the master-replica connection is in progress), `sync` (the master and replica are trying to perform the synchronization), `connected` (the replica is online).
5. The amount of data received from the replica so far in terms of master replication offset.

---

### SLAVEOF

#### Syntax

```bash
SLAVEOF <host port | NO ONE>
```

The SLAVEOF command can change the replication settings of a slave on the fly.

#### Resp Reply

Simple string reply: OK.

---

### SWAPDB

#### Syntax

```bash
SWAPDB index1 index2
```

This command swaps two Garnet databases, so that immediately all the clients connected to a given database will see the data of the other database, and the other way around.

#### Resp Reply

Simple string reply: OK.

---

### TIME
#### Syntax

```bash
TIME
```

The TIME command returns the current server time as a two items lists: a Unix timestamp and the amount of microseconds already elapsed in the current second. Basically the interface is very similar to the one of the gettimeofday system call.

#### Resp Reply

Array reply: specifically, a two-element array consisting of the Unix timestamp in seconds and the microseconds' count.

---

### MONITOR

#### Syntax

```bash
MONITOR
```

MONITOR is a debugging command that streams back every command processed by the Redis server. It can help in understanding what is happening to the database.

#### Resp Reply

Non-standard return value. Dumps the received commands in an infinite flow.

---
